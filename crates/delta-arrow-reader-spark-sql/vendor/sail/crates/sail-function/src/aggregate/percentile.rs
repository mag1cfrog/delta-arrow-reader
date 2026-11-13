use std::fmt::Debug;
use std::sync::Arc;

use datafusion::arrow;
use datafusion::arrow::array::{Array, ArrayRef, AsArray, RecordBatch, RecordBatchOptions};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::cast::{
    as_float64_array, as_int64_array, as_string_array, as_uint64_array,
};
use datafusion::common::{HashMap, ScalarValue};
use datafusion::error::Result;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility};
use datafusion::physical_expr::PhysicalExpr;
use datafusion_common::DataFusionError;
use datafusion_expr_common::columnar_value::ColumnarValue;
use ordered_float::OrderedFloat;
use sail_common_datafusion::literal::LiteralValue;

/// The `PercentileFunction` calculates the exact percentile (quantile) from a set of values.
///
/// Unlike `approx_percentile_cont`, this function calculates the exact percentile by:
/// 1. Collecting all values
/// 2. Sorting them
/// 3. Computing the exact percentile using linear interpolation (for numeric types)
///    or selecting the value at the position (for string types)
///
/// This is computationally more expensive than the approximate version but provides exact results.
///
/// For the special case of percentile 0.5 (median), this is equivalent to the `median` function.
///
/// Supported types:
/// - Numeric types (Int, Float, etc.) -> returns Float64 with linear interpolation
/// - String types (Utf8, Utf8View, LargeUtf8) -> returns the same string type
/// - Interval types -> returns the same interval type
/// - Duration types -> returns the same duration type
#[derive(PartialEq, Eq, Hash)]
pub struct PercentileFunction {
    signature: Signature,
}

impl Debug for PercentileFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PercentileFunction")
            .field("signature", &self.signature)
            .finish()
    }
}

impl Default for PercentileFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl PercentileFunction {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for PercentileFunction {
    fn name(&self) -> &str {
        "percentile"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        // Check if second argument is an array (multiple percentiles)
        let is_array_percentiles = matches!(arg_types.get(1), Some(DataType::List(_)));

        let base_type = match &arg_types[0] {
            DataType::Utf8 => DataType::Utf8,
            DataType::Utf8View => DataType::Utf8View,
            DataType::LargeUtf8 => DataType::LargeUtf8,
            dt @ DataType::Interval(_) => dt.clone(),
            dt @ DataType::Duration(_) => dt.clone(),
            _ => DataType::Float64,
        };

        // If percentiles is an array, return List of base_type
        if is_array_percentiles {
            Ok(DataType::List(Arc::new(Field::new(
                "item", base_type, true,
            ))))
        } else {
            Ok(base_type)
        }
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let data_type = &acc_args.exprs[0].data_type(acc_args.schema)?;

        let expr: &Arc<dyn PhysicalExpr> = acc_args.exprs.get(1).ok_or_else(|| {
            DataFusionError::Execution(
                "percentile() requires a second argument (percentile value)".into(),
            )
        })?;

        // Try to extract as array of percentiles first
        match extract_percentiles_array(expr) {
            Ok(percentiles) => {
                // Multiple percentiles - create MultiPercentileAccumulator
                // Validate all percentiles are within [0.0, 1.0]
                for percentile in &percentiles {
                    if !(0.0..=1.0).contains(percentile) {
                        return Err(DataFusionError::Execution(format!(
                            "Percentile value {} is out of range [0.0, 1.0]",
                            percentile
                        )));
                    }
                }

                match data_type {
                    DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => Ok(Box::new(
                        MultiStringPercentileAccumulator::new(percentiles, data_type.clone()),
                    )),
                    DataType::Interval(_) | DataType::Duration(_) => Ok(Box::new(
                        MultiIntervalPercentileAccumulator::new(percentiles, data_type.clone()),
                    )),
                    _ => Ok(Box::new(NumericPercentileAccumulator::with_percentiles(
                        percentiles,
                        true,
                    ))),
                }
            }
            _ => {
                // Single percentile - existing behavior
                let percentile: f64 = extract_literal(expr).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to extract percentile value in percentile(): {}",
                        e
                    ))
                })?;

                // Validate that percentile is within [0.0, 1.0]
                if !(0.0..=1.0).contains(&percentile) {
                    return Err(DataFusionError::Execution(format!(
                        "Percentile value {} is out of range [0.0, 1.0]",
                        percentile
                    )));
                }

                match data_type {
                    DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => Ok(Box::new(
                        StringPercentileAccumulator::new(percentile, data_type.clone()),
                    )),
                    DataType::Interval(_) | DataType::Duration(_) => Ok(Box::new(
                        IntervalPercentileAccumulator::new(percentile, data_type.clone()),
                    )),
                    _ => Ok(Box::new(NumericPercentileAccumulator::new(percentile))),
                }
            }
        }
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<arrow::datatypes::FieldRef>> {
        let value_type = args.input_fields[0].data_type().clone();

        let storage_type = match &value_type {
            DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8 => DataType::Utf8,
            DataType::Interval(_) | DataType::Duration(_) => DataType::Int64,
            _ => DataType::Float64,
        };

        let values_list_type =
            DataType::List(Arc::new(Field::new("item", storage_type.clone(), true)));

        let mut fields = vec![Field::new("values", values_list_type, true).into()];
        if storage_type == DataType::Float64 {
            let counts_type = DataType::List(Arc::new(Field::new("item", DataType::UInt64, true)));
            fields.push(Field::new("counts", counts_type, true).into());
        }
        Ok(fields)
    }
}

/// Numeric percentiles use value counts for weighted ranks and window retraction.
#[derive(Debug)]
pub struct NumericPercentileAccumulator {
    value_counts: HashMap<OrderedFloat<f64>, u64>,
    total_count: u64,
    percentiles: Vec<f64>,
    returns_array: bool,
}

impl NumericPercentileAccumulator {
    pub fn new(percentile: f64) -> Self {
        Self::with_percentiles(vec![percentile], false)
    }

    fn with_percentiles(percentiles: Vec<f64>, returns_array: bool) -> Self {
        Self {
            value_counts: HashMap::new(),
            total_count: 0,
            percentiles,
            returns_array,
        }
    }

    fn sorted_cumulative_counts(&self) -> Vec<(OrderedFloat<f64>, u64)> {
        let mut counts: Vec<_> = self.value_counts.iter().map(|(&v, &c)| (v, c)).collect();
        counts.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        let mut cumulative = 0;
        for (_, count) in &mut counts {
            cumulative += *count;
            *count = cumulative;
        }
        counts
    }

    fn calculate_percentile(
        &self,
        cumulative_counts: &[(OrderedFloat<f64>, u64)],
        percentile: f64,
    ) -> Option<f64> {
        if self.total_count == 0 {
            return None;
        }
        let last_rank = self.total_count - 1;
        let position = last_rank as f64 * percentile;
        let lower_rank = (position.floor() as u64).min(last_rank);
        let upper_rank = (position.ceil() as u64).min(last_rank);
        let lower_value = cumulative_counts
            [cumulative_counts.partition_point(|(_, count)| *count <= lower_rank)]
        .0
        .into_inner();
        if lower_rank == upper_rank {
            return Some(lower_value);
        }
        let upper_value = cumulative_counts
            [cumulative_counts.partition_point(|(_, count)| *count <= upper_rank)]
        .0
        .into_inner();
        let fraction = position - lower_rank as f64;
        Some(lower_value + fraction * (upper_value - lower_value))
    }

    fn apply_count(&mut self, value: f64, frequency: u64, retract: bool) -> Result<()> {
        if frequency == 0 {
            return Ok(());
        }
        let value = OrderedFloat(value);
        if retract {
            if let Some(count) = self.value_counts.get_mut(&value) {
                let remaining = count.checked_sub(frequency).ok_or_else(|| {
                    DataFusionError::Execution("Percentile frequency underflow".into())
                })?;
                self.total_count -= frequency;
                if remaining == 0 {
                    self.value_counts.remove(&value);
                } else {
                    *count = remaining;
                }
            }
        } else {
            let total = self.total_count.checked_add(frequency).ok_or_else(|| {
                DataFusionError::Execution("Percentile frequency overflow".into())
            })?;
            *self.value_counts.entry(value).or_insert(0) += frequency;
            self.total_count = total;
        }
        Ok(())
    }

    fn apply_batch(&mut self, values: &[ArrayRef], retract: bool) -> Result<()> {
        let Some(array) = values.first() else {
            return Ok(());
        };
        let float_array = arrow::compute::cast(array, &DataType::Float64)?;
        let float_array = as_float64_array(&float_array)?;
        let frequencies = values
            .get(2)
            .map(|array| arrow::compute::cast(array, &DataType::Int64))
            .transpose()?;
        let frequencies = frequencies.as_deref().map(as_int64_array).transpose()?;
        if frequencies.is_some_and(|array| array.len() != float_array.len()) {
            return Err(DataFusionError::Execution(
                "Percentile values and frequencies have different lengths".into(),
            ));
        }
        for (row, value) in float_array.iter().enumerate() {
            let frequency = match frequencies {
                Some(array) if array.is_null(row) => continue,
                Some(array) => array.value(row),
                None => 1,
            };
            let frequency = u64::try_from(frequency).map_err(|_| {
                DataFusionError::Execution("Percentile frequency must be nonnegative".into())
            })?;
            if let Some(value) = value {
                self.apply_count(value, frequency, retract)?;
            }
        }
        Ok(())
    }
}

impl Accumulator for NumericPercentileAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.apply_batch(values, false)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let counts = self.sorted_cumulative_counts();
        if !self.returns_array {
            return Ok(ScalarValue::Float64(
                self.calculate_percentile(&counts, self.percentiles[0]),
            ));
        }
        if self.total_count == 0 {
            return Ok(ScalarValue::new_null_list(DataType::Float64, true, 1));
        }
        let results: Vec<_> = self
            .percentiles
            .iter()
            .map(|&percentile| ScalarValue::Float64(self.calculate_percentile(&counts, percentile)))
            .collect();
        Ok(ScalarValue::List(ScalarValue::new_list_nullable(
            &results,
            &DataType::Float64,
        )))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self.value_counts.capacity() * std::mem::size_of::<(OrderedFloat<f64>, u64)>()
            + self.percentiles.capacity() * std::mem::size_of::<f64>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let mut values = Vec::with_capacity(self.value_counts.len());
        let mut counts = Vec::with_capacity(self.value_counts.len());
        for (value, count) in &self.value_counts {
            values.push(ScalarValue::Float64(Some(value.into_inner())));
            counts.push(ScalarValue::UInt64(Some(*count)));
        }
        Ok(vec![
            ScalarValue::List(ScalarValue::new_list_nullable(&values, &DataType::Float64)),
            ScalarValue::List(ScalarValue::new_list_nullable(&counts, &DataType::UInt64)),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }
        let (Some(values), Some(counts)) = (
            states.first().and_then(|array| array.as_list_opt::<i32>()),
            states.get(1).and_then(|array| array.as_list_opt::<i32>()),
        ) else {
            return Err(DataFusionError::Execution(
                "Invalid percentile values/counts state".into(),
            ));
        };
        if values.len() != counts.len() || values.null_count() != counts.null_count() {
            return Err(DataFusionError::Execution(
                "Mismatched percentile values/counts state".into(),
            ));
        }
        for row in 0..values.len() {
            if values.is_null(row) && counts.is_null(row) {
                continue;
            }
            let value_array = values.value(row);
            let count_array = counts.value(row);
            let value_array = as_float64_array(&value_array)?;
            let count_array = as_uint64_array(&count_array)?;
            if values.is_null(row)
                || counts.is_null(row)
                || value_array.len() != count_array.len()
                || value_array.null_count() != 0
                || count_array.null_count() != 0
            {
                return Err(DataFusionError::Execution(
                    "Mismatched percentile values/counts state".into(),
                ));
            }
            for (value, count) in value_array.values().iter().zip(count_array.values()) {
                self.apply_count(*value, *count, false)?;
            }
        }
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.apply_batch(values, true)
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }
}

/// Accumulator for string types (returns the value at the percentile position)
///
/// Optimization: Uses HashMap to track string counts for O(1) retract operations
#[derive(Debug)]
pub struct StringPercentileAccumulator {
    value_counts: HashMap<String, usize>,
    total_count: usize,
    percentile: f64,
    data_type: DataType,
}

impl StringPercentileAccumulator {
    pub fn new(percentile: f64, data_type: DataType) -> Self {
        Self {
            value_counts: HashMap::new(),
            total_count: 0,
            percentile,
            data_type,
        }
    }

    /// Calculate percentile for strings (no interpolation, uses rounding)
    fn calculate_percentile(&self, sorted_values: &[String]) -> Option<String> {
        if sorted_values.is_empty() {
            return None;
        }

        if sorted_values.len() == 1 {
            return Some(sorted_values[0].clone());
        }

        let n: usize = sorted_values.len();
        let pos: f64 = (n - 1) as f64 * self.percentile;
        let idx: usize = pos.round() as usize;

        Some(sorted_values[idx].clone())
    }

    /// Convert HashMap to sorted Vec
    fn get_sorted_values(&self) -> Vec<String> {
        let mut values = Vec::with_capacity(self.total_count);

        for (val, &count) in &self.value_counts {
            for _ in 0..count {
                values.push(val.clone());
            }
        }

        values.sort();
        values
    }
}

impl Accumulator for StringPercentileAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let array = &values[0];

        // Convert input to Utf8 and collect values
        let string_array = arrow::compute::cast(array, &DataType::Utf8)?;
        let string_array = as_string_array(&string_array)?;

        for value in string_array.iter().flatten() {
            let val = value.to_string();
            *self.value_counts.entry(val).or_insert(0) += 1;
            self.total_count += 1;
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.total_count == 0 {
            return match_string_type!(&self.data_type, None);
        }

        let sorted_values = self.get_sorted_values();

        match self.calculate_percentile(&sorted_values) {
            Some(result) => match_string_type!(&self.data_type, Some(result)),
            None => match_string_type!(&self.data_type, None),
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(&self.value_counts)
            + self
                .value_counts
                .keys()
                .map(|k| k.capacity() + std::mem::size_of::<usize>())
                .sum::<usize>()
            + std::mem::size_of::<usize>()
            + std::mem::size_of::<f64>()
            + std::mem::size_of::<DataType>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let sorted_values = self.get_sorted_values();

        let values_scalar = ScalarValue::new_list_nullable(
            &sorted_values
                .iter()
                .map(|v| ScalarValue::Utf8(Some(v.clone())))
                .collect::<Vec<_>>(),
            &DataType::Utf8,
        );

        Ok(vec![ScalarValue::List(values_scalar)])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        let values_list = &states[0];

        if let Some(list_array) = values_list.as_list_opt::<i32>() {
            for list_idx in 0..list_array.len() {
                if !list_array.is_null(list_idx) {
                    let value_array = list_array.value(list_idx);
                    let string_array = as_string_array(&value_array)?;

                    for value in string_array.iter().flatten() {
                        let val = value.to_string();
                        *self.value_counts.entry(val).or_insert(0) += 1;
                        self.total_count += 1;
                    }
                }
            }
        }

        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let array = &values[0];

        let string_array = arrow::compute::cast(array, &DataType::Utf8)?;
        let string_array = as_string_array(&string_array)?;

        // O(1) removal using HashMap
        for v in string_array.iter().flatten() {
            let val = v.to_string();
            if let Some(count) = self.value_counts.get_mut(&val) {
                *count -= 1;
                self.total_count -= 1;

                if *count == 0 {
                    self.value_counts.remove(&val);
                }
            }
        }

        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }
}

/// Accumulator for multiple string percentiles (returns array of results)
#[derive(Debug)]
pub struct MultiStringPercentileAccumulator {
    value_counts: HashMap<String, usize>,
    total_count: usize,
    percentiles: Vec<f64>,
    data_type: DataType,
}

impl MultiStringPercentileAccumulator {
    pub fn new(percentiles: Vec<f64>, data_type: DataType) -> Self {
        Self {
            value_counts: HashMap::new(),
            total_count: 0,
            percentiles,
            data_type,
        }
    }

    fn calculate_single_percentile(sorted_values: &[String], percentile: f64) -> Option<String> {
        if sorted_values.is_empty() {
            return None;
        }

        if sorted_values.len() == 1 {
            return Some(sorted_values[0].clone());
        }

        let n = sorted_values.len();
        let pos = (n - 1) as f64 * percentile;
        let idx = pos.round() as usize;
        Some(sorted_values[idx].clone())
    }

    fn get_sorted_values(&self) -> Vec<String> {
        let mut values = Vec::with_capacity(self.total_count);

        for (string_val, &count) in &self.value_counts {
            for _ in 0..count {
                values.push(string_val.clone());
            }
        }

        values.sort();
        values
    }
}

impl Accumulator for MultiStringPercentileAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let array = &values[0];

        let string_array = arrow::compute::cast(array, &DataType::Utf8)?;
        let string_array = as_string_array(&string_array)?;

        for value in string_array.iter().flatten() {
            *self.value_counts.entry(value.to_string()).or_insert(0) += 1;
            self.total_count += 1;
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.total_count == 0 {
            // Return NULL list
            return Ok(ScalarValue::new_null_list(
                self.data_type.clone(),
                true,
                1, // field_count
            ));
        }

        let sorted_values = self.get_sorted_values();
        let mut results = Vec::with_capacity(self.percentiles.len());

        for &percentile in &self.percentiles {
            let result = Self::calculate_single_percentile(&sorted_values, percentile);
            let scalar = match &self.data_type {
                DataType::Utf8View => ScalarValue::Utf8View(result),
                DataType::LargeUtf8 => ScalarValue::LargeUtf8(result),
                _ => ScalarValue::Utf8(result),
            };
            results.push(scalar);
        }

        Ok(ScalarValue::List(ScalarValue::new_list_nullable(
            &results,
            &self.data_type,
        )))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(&self.value_counts)
            + self.value_counts.keys().map(|k| k.len()).sum::<usize>()
            + std::mem::size_of::<usize>()
            + self.percentiles.len() * std::mem::size_of::<f64>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let sorted_values = self.get_sorted_values();
        let values_scalar = ScalarValue::new_list_nullable(
            &sorted_values
                .iter()
                .map(|v| ScalarValue::Utf8(Some(v.clone())))
                .collect::<Vec<_>>(),
            &DataType::Utf8,
        );

        Ok(vec![ScalarValue::List(values_scalar)])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        let values_list = &states[0];

        if let Some(list_array) = values_list.as_list_opt::<i32>() {
            for list_idx in 0..list_array.len() {
                if !list_array.is_null(list_idx) {
                    let value_array = list_array.value(list_idx);
                    let string_array = as_string_array(&value_array)?;

                    for value in string_array.iter().flatten() {
                        *self.value_counts.entry(value.to_string()).or_insert(0) += 1;
                        self.total_count += 1;
                    }
                }
            }
        }

        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let array = &values[0];

        let string_array = arrow::compute::cast(array, &DataType::Utf8)?;
        let string_array = as_string_array(&string_array)?;

        for v in string_array.iter().flatten() {
            let key = v.to_string();
            if let Some(count) = self.value_counts.get_mut(&key) {
                *count -= 1;
                self.total_count -= 1;

                if *count == 0 {
                    self.value_counts.remove(&key);
                }
            }
        }

        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }
}

/// Accumulator for interval/duration types
///
/// Optimization: Uses HashMap to track interval counts for O(1) retract operations
#[derive(Debug)]
pub struct IntervalPercentileAccumulator {
    value_counts: HashMap<i64, usize>,
    total_count: usize,
    percentile: f64,
    data_type: DataType,
}

impl IntervalPercentileAccumulator {
    pub fn new(percentile: f64, data_type: DataType) -> Self {
        Self {
            value_counts: HashMap::new(),
            total_count: 0,
            percentile,
            data_type,
        }
    }

    /// Calculate percentile for i64 with interpolation
    fn calculate_percentile(&self, sorted_values: &[i64]) -> Option<i64> {
        if sorted_values.is_empty() {
            return None;
        }

        if sorted_values.len() == 1 {
            return Some(sorted_values[0]);
        }

        let n: usize = sorted_values.len();
        let pos: f64 = (n - 1) as f64 * self.percentile;
        let lower_idx: usize = pos.floor() as usize;
        let upper_idx: usize = pos.ceil() as usize;

        if lower_idx == upper_idx {
            Some(sorted_values[lower_idx])
        } else {
            let lower: f64 = sorted_values[lower_idx] as f64;
            let upper: f64 = sorted_values[upper_idx] as f64;
            let fraction: f64 = pos - lower_idx as f64;
            Some((lower + fraction * (upper - lower)).round() as i64)
        }
    }

    /// Convert HashMap to sorted Vec
    fn get_sorted_values(&self) -> Vec<i64> {
        let mut values = Vec::with_capacity(self.total_count);

        for (&val, &count) in &self.value_counts {
            for _ in 0..count {
                values.push(val);
            }
        }

        values.sort_unstable();
        values
    }
}

impl Accumulator for IntervalPercentileAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let array = &values[0];

        for row_idx in 0..array.len() {
            if array.is_null(row_idx) {
                continue;
            }

            // Convert interval/duration to i64 for calculation
            let value = match &self.data_type {
                DataType::Interval(unit) => {
                    use datafusion::arrow::datatypes::IntervalUnit;
                    match unit {
                        IntervalUnit::YearMonth => {
                            let arr = array.as_primitive::<datafusion::arrow::datatypes::IntervalYearMonthType>();
                            arr.value(row_idx) as i64
                        }
                        IntervalUnit::DayTime => {
                            let arr = array
                                .as_primitive::<datafusion::arrow::datatypes::IntervalDayTimeType>(
                                );
                            let val = arr.value(row_idx);
                            // IntervalDayTime is stored as (days, milliseconds)
                            // Convert to total milliseconds for interpolation
                            val.days as i64 * 86_400_000 + val.milliseconds as i64
                        }
                        IntervalUnit::MonthDayNano => {
                            let arr = array.as_primitive::<datafusion::arrow::datatypes::IntervalMonthDayNanoType>();
                            let val = arr.value(row_idx);
                            // Convert to nanoseconds for interpolation
                            val.months as i64 * 2_592_000_000_000_000 // ~30 days in nanos
                                + val.days as i64 * 86_400_000_000_000 // days to nanos
                                + val.nanoseconds
                        }
                    }
                }
                DataType::Duration(unit) => {
                    use datafusion::arrow::datatypes::TimeUnit;
                    match unit {
                        TimeUnit::Second => array
                            .as_primitive::<datafusion::arrow::datatypes::DurationSecondType>()
                            .value(row_idx),
                        TimeUnit::Millisecond => array
                            .as_primitive::<datafusion::arrow::datatypes::DurationMillisecondType>()
                            .value(row_idx),
                        TimeUnit::Microsecond => array
                            .as_primitive::<datafusion::arrow::datatypes::DurationMicrosecondType>()
                            .value(row_idx),
                        TimeUnit::Nanosecond => array
                            .as_primitive::<datafusion::arrow::datatypes::DurationNanosecondType>()
                            .value(row_idx),
                    }
                }
                _ => {
                    return Err(DataFusionError::Execution(format!(
                        "IntervalPercentileAccumulator does not support type {:?}",
                        self.data_type
                    )));
                }
            };

            *self.value_counts.entry(value).or_insert(0) += 1;
            self.total_count += 1;
        }

        Ok(())
    }
    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.total_count == 0 {
            return match &self.data_type {
                DataType::Interval(unit) => interval_none!(unit),
                DataType::Duration(unit) => duration_none!(unit),
                _ => Err(DataFusionError::Execution(format!(
                    "Unsupported type {:?}",
                    self.data_type
                ))),
            };
        }

        let sorted_values = self.get_sorted_values();

        match self.calculate_percentile(&sorted_values) {
            Some(result_i64) => match &self.data_type {
                DataType::Interval(unit) => {
                    use datafusion::arrow::datatypes::IntervalUnit;
                    match unit {
                        IntervalUnit::YearMonth => {
                            Ok(ScalarValue::IntervalYearMonth(Some(result_i64 as i32)))
                        }
                        IntervalUnit::DayTime => {
                            // Convert back from milliseconds to (days, milliseconds)
                            use datafusion::arrow::datatypes::IntervalDayTime;
                            let days = (result_i64 / 86_400_000) as i32;
                            let milliseconds = (result_i64 % 86_400_000) as i32;
                            Ok(ScalarValue::IntervalDayTime(Some(IntervalDayTime {
                                days,
                                milliseconds,
                            })))
                        }
                        IntervalUnit::MonthDayNano => {
                            // Convert back from nanoseconds
                            use datafusion::arrow::datatypes::IntervalMonthDayNano;
                            let months = (result_i64 / 2_592_000_000_000_000) as i32;
                            let remaining = result_i64 % 2_592_000_000_000_000;
                            let days = (remaining / 86_400_000_000_000) as i32;
                            let nanoseconds = remaining % 86_400_000_000_000;
                            Ok(ScalarValue::IntervalMonthDayNano(Some(
                                IntervalMonthDayNano {
                                    months,
                                    days,
                                    nanoseconds,
                                },
                            )))
                        }
                    }
                }
                DataType::Duration(unit) => {
                    use datafusion::arrow::datatypes::TimeUnit;
                    match unit {
                        TimeUnit::Second => Ok(ScalarValue::DurationSecond(Some(result_i64))),
                        TimeUnit::Millisecond => {
                            Ok(ScalarValue::DurationMillisecond(Some(result_i64)))
                        }
                        TimeUnit::Microsecond => {
                            Ok(ScalarValue::DurationMicrosecond(Some(result_i64)))
                        }
                        TimeUnit::Nanosecond => {
                            Ok(ScalarValue::DurationNanosecond(Some(result_i64)))
                        }
                    }
                }
                _ => Err(DataFusionError::Execution(format!(
                    "Unsupported type {:?}",
                    self.data_type
                ))),
            },
            None => match &self.data_type {
                DataType::Interval(unit) => interval_none!(unit),
                DataType::Duration(unit) => duration_none!(unit),
                _ => Err(DataFusionError::Execution(format!(
                    "Unsupported type {:?}",
                    self.data_type
                ))),
            },
        }
    }
    fn size(&self) -> usize {
        std::mem::size_of_val(&self.value_counts)
            + self.value_counts.capacity() * std::mem::size_of::<(i64, usize)>()
            + std::mem::size_of::<usize>()
            + std::mem::size_of::<f64>()
            + std::mem::size_of::<DataType>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let sorted_values = self.get_sorted_values();

        let values_scalar = ScalarValue::new_list_nullable(
            &sorted_values
                .iter()
                .map(|&v| ScalarValue::Int64(Some(v)))
                .collect::<Vec<_>>(),
            &DataType::Int64,
        );

        Ok(vec![ScalarValue::List(values_scalar)])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        let values_list = states[0].as_list::<i32>();

        for list_idx in 0..values_list.len() {
            if values_list.is_null(list_idx) {
                continue;
            }

            let values_array = values_list.value(list_idx);
            let int_array = values_array.as_primitive::<datafusion::arrow::datatypes::Int64Type>();

            for elem_idx in 0..int_array.len() {
                if !int_array.is_null(elem_idx) {
                    let value = int_array.value(elem_idx);
                    *self.value_counts.entry(value).or_insert(0) += 1;
                    self.total_count += 1;
                }
            }
        }

        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let array = &values[0];

        for row_idx in 0..array.len() {
            if array.is_null(row_idx) {
                continue;
            }

            let val_i64 = match &self.data_type {
                DataType::Interval(unit) => {
                    use datafusion::arrow::datatypes::IntervalUnit;
                    match unit {
                        IntervalUnit::YearMonth => {
                            let arr = array.as_primitive::<datafusion::arrow::datatypes::IntervalYearMonthType>();
                            arr.value(row_idx) as i64
                        }
                        IntervalUnit::DayTime => {
                            let arr = array
                                .as_primitive::<datafusion::arrow::datatypes::IntervalDayTimeType>(
                                );
                            let v = arr.value(row_idx);
                            v.days as i64 * 86_400_000 + v.milliseconds as i64
                        }
                        IntervalUnit::MonthDayNano => {
                            let arr = array.as_primitive::<datafusion::arrow::datatypes::IntervalMonthDayNanoType>();
                            let v = arr.value(row_idx);
                            v.months as i64 * 2_592_000_000_000_000
                                + v.days as i64 * 86_400_000_000_000
                                + v.nanoseconds
                        }
                    }
                }
                DataType::Duration(unit) => {
                    use datafusion::arrow::datatypes::TimeUnit;
                    match unit {
                        TimeUnit::Second => array
                            .as_primitive::<datafusion::arrow::datatypes::DurationSecondType>()
                            .value(row_idx),
                        TimeUnit::Millisecond => array
                            .as_primitive::<datafusion::arrow::datatypes::DurationMillisecondType>()
                            .value(row_idx),
                        TimeUnit::Microsecond => array
                            .as_primitive::<datafusion::arrow::datatypes::DurationMicrosecondType>()
                            .value(row_idx),
                        TimeUnit::Nanosecond => array
                            .as_primitive::<datafusion::arrow::datatypes::DurationNanosecondType>()
                            .value(row_idx),
                    }
                }
                _ => {
                    return Err(DataFusionError::Execution(format!(
                        "IntervalPercentileAccumulator does not support type {:?}",
                        self.data_type
                    )));
                }
            };

            // O(1) removal using HashMap
            if let Some(count) = self.value_counts.get_mut(&val_i64) {
                *count -= 1;
                self.total_count -= 1;

                if *count == 0 {
                    self.value_counts.remove(&val_i64);
                }
            }
        }

        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }
}

/// Accumulator for multiple interval/duration percentiles (returns array of results)
#[derive(Debug)]
pub struct MultiIntervalPercentileAccumulator {
    value_counts: HashMap<i64, usize>,
    total_count: usize,
    percentiles: Vec<f64>,
    data_type: DataType,
}

impl MultiIntervalPercentileAccumulator {
    pub fn new(percentiles: Vec<f64>, data_type: DataType) -> Self {
        Self {
            value_counts: HashMap::new(),
            total_count: 0,
            percentiles,
            data_type,
        }
    }

    fn calculate_single_percentile(sorted_values: &[i64], percentile: f64) -> Option<i64> {
        if sorted_values.is_empty() {
            return None;
        }

        if sorted_values.len() == 1 {
            return Some(sorted_values[0]);
        }

        let n = sorted_values.len();
        let pos = (n - 1) as f64 * percentile;
        let lower_idx = pos.floor() as usize;
        let upper_idx = pos.ceil() as usize;

        if lower_idx == upper_idx {
            Some(sorted_values[lower_idx])
        } else {
            let lower_val = sorted_values[lower_idx] as f64;
            let upper_val = sorted_values[upper_idx] as f64;
            let fraction = pos - lower_idx as f64;
            Some((lower_val + fraction * (upper_val - lower_val)).round() as i64)
        }
    }

    fn get_sorted_values(&self) -> Vec<i64> {
        let mut values = Vec::with_capacity(self.total_count);

        for (&val, &count) in &self.value_counts {
            for _ in 0..count {
                values.push(val);
            }
        }

        values.sort_unstable();
        values
    }

    fn i64_to_scalar(&self, result_i64: i64) -> Result<ScalarValue> {
        match &self.data_type {
            DataType::Interval(unit) => {
                use datafusion::arrow::datatypes::IntervalUnit;
                match unit {
                    IntervalUnit::YearMonth => {
                        Ok(ScalarValue::IntervalYearMonth(Some(result_i64 as i32)))
                    }
                    IntervalUnit::DayTime => {
                        use datafusion::arrow::datatypes::IntervalDayTime;
                        let days = (result_i64 / 86_400_000) as i32;
                        let milliseconds = (result_i64 % 86_400_000) as i32;
                        Ok(ScalarValue::IntervalDayTime(Some(IntervalDayTime {
                            days,
                            milliseconds,
                        })))
                    }
                    IntervalUnit::MonthDayNano => {
                        use datafusion::arrow::datatypes::IntervalMonthDayNano;
                        let months = (result_i64 / 2_592_000_000_000_000) as i32;
                        let remaining = result_i64 % 2_592_000_000_000_000;
                        let days = (remaining / 86_400_000_000_000) as i32;
                        let nanoseconds = remaining % 86_400_000_000_000;
                        Ok(ScalarValue::IntervalMonthDayNano(Some(
                            IntervalMonthDayNano {
                                months,
                                days,
                                nanoseconds,
                            },
                        )))
                    }
                }
            }
            DataType::Duration(unit) => {
                use datafusion::arrow::datatypes::TimeUnit;
                match unit {
                    TimeUnit::Second => Ok(ScalarValue::DurationSecond(Some(result_i64))),
                    TimeUnit::Millisecond => Ok(ScalarValue::DurationMillisecond(Some(result_i64))),
                    TimeUnit::Microsecond => Ok(ScalarValue::DurationMicrosecond(Some(result_i64))),
                    TimeUnit::Nanosecond => Ok(ScalarValue::DurationNanosecond(Some(result_i64))),
                }
            }
            _ => Err(DataFusionError::Execution(format!(
                "Unexpected data type for interval percentile: {:?}",
                self.data_type
            ))),
        }
    }
}

impl Accumulator for MultiIntervalPercentileAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let array = &values[0];

        // Convert intervals/durations to i64 for sorting
        for row_idx in 0..array.len() {
            if !array.is_null(row_idx) {
                let value_i64 = match &self.data_type {
                    DataType::Interval(unit) => {
                        use datafusion::arrow::datatypes::IntervalUnit;
                        match unit {
                            IntervalUnit::YearMonth => {
                                let arr = array.as_primitive::<datafusion::arrow::datatypes::IntervalYearMonthType>();
                                arr.value(row_idx) as i64
                            }
                            IntervalUnit::DayTime => {
                                let arr = array.as_primitive::<datafusion::arrow::datatypes::IntervalDayTimeType>();
                                let val = arr.value(row_idx);
                                val.days as i64 * 86_400_000 + val.milliseconds as i64
                            }
                            IntervalUnit::MonthDayNano => {
                                let arr = array.as_primitive::<datafusion::arrow::datatypes::IntervalMonthDayNanoType>();
                                let val = arr.value(row_idx);
                                val.months as i64 * 2_592_000_000_000_000
                                    + val.days as i64 * 86_400_000_000_000
                                    + val.nanoseconds
                            }
                        }
                    }
                    DataType::Duration(unit) => {
                        use datafusion::arrow::datatypes::TimeUnit;
                        match unit {
                            TimeUnit::Second => {
                                let arr = array.as_primitive::<datafusion::arrow::datatypes::DurationSecondType>();
                                arr.value(row_idx)
                            }
                            TimeUnit::Millisecond => {
                                let arr = array.as_primitive::<datafusion::arrow::datatypes::DurationMillisecondType>();
                                arr.value(row_idx)
                            }
                            TimeUnit::Microsecond => {
                                let arr = array.as_primitive::<datafusion::arrow::datatypes::DurationMicrosecondType>();
                                arr.value(row_idx)
                            }
                            TimeUnit::Nanosecond => {
                                let arr = array.as_primitive::<datafusion::arrow::datatypes::DurationNanosecondType>();
                                arr.value(row_idx)
                            }
                        }
                    }
                    _ => {
                        return Err(DataFusionError::Execution(format!(
                            "Unexpected data type for interval percentile: {:?}",
                            self.data_type
                        )));
                    }
                };

                *self.value_counts.entry(value_i64).or_insert(0) += 1;
                self.total_count += 1;
            }
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.total_count == 0 {
            // Return NULL list
            return Ok(ScalarValue::new_null_list(
                self.data_type.clone(),
                true,
                1, // field_count
            ));
        }

        let sorted_values = self.get_sorted_values();
        let mut results = Vec::with_capacity(self.percentiles.len());

        for &percentile in &self.percentiles {
            if let Some(result_i64) = Self::calculate_single_percentile(&sorted_values, percentile)
            {
                let scalar = self.i64_to_scalar(result_i64)?;
                results.push(scalar);
            } else {
                // Return NULL for empty result
                let null_scalar = match &self.data_type {
                    DataType::Interval(unit) => {
                        use datafusion::arrow::datatypes::IntervalUnit;
                        match unit {
                            IntervalUnit::YearMonth => ScalarValue::IntervalYearMonth(None),
                            IntervalUnit::DayTime => ScalarValue::IntervalDayTime(None),
                            IntervalUnit::MonthDayNano => ScalarValue::IntervalMonthDayNano(None),
                        }
                    }
                    DataType::Duration(unit) => {
                        use datafusion::arrow::datatypes::TimeUnit;
                        match unit {
                            TimeUnit::Second => ScalarValue::DurationSecond(None),
                            TimeUnit::Millisecond => ScalarValue::DurationMillisecond(None),
                            TimeUnit::Microsecond => ScalarValue::DurationMicrosecond(None),
                            TimeUnit::Nanosecond => ScalarValue::DurationNanosecond(None),
                        }
                    }
                    _ => {
                        return Err(DataFusionError::Execution(format!(
                            "Unexpected data type: {:?}",
                            self.data_type
                        )));
                    }
                };
                results.push(null_scalar);
            }
        }

        Ok(ScalarValue::List(ScalarValue::new_list_nullable(
            &results,
            &self.data_type,
        )))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(&self.value_counts)
            + self.value_counts.capacity() * std::mem::size_of::<(i64, usize)>()
            + std::mem::size_of::<usize>()
            + self.percentiles.len() * std::mem::size_of::<f64>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let sorted_values = self.get_sorted_values();
        let values_scalar = ScalarValue::new_list_nullable(
            &sorted_values
                .iter()
                .map(|&v| ScalarValue::Int64(Some(v)))
                .collect::<Vec<_>>(),
            &DataType::Int64,
        );

        Ok(vec![ScalarValue::List(values_scalar)])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        let values_list = &states[0];

        if let Some(list_array) = values_list.as_list_opt::<i32>() {
            for list_idx in 0..list_array.len() {
                if !list_array.is_null(list_idx) {
                    let value_array = list_array.value(list_idx);

                    if let Some(int64_array) =
                        value_array.as_primitive_opt::<datafusion::arrow::datatypes::Int64Type>()
                    {
                        for value in int64_array.iter().flatten() {
                            *self.value_counts.entry(value).or_insert(0) += 1;
                            self.total_count += 1;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let array = &values[0];

        for row_idx in 0..array.len() {
            if !array.is_null(row_idx) {
                let value_i64 = match &self.data_type {
                    DataType::Interval(unit) => {
                        use datafusion::arrow::datatypes::IntervalUnit;
                        match unit {
                            IntervalUnit::YearMonth => {
                                let arr = array.as_primitive::<datafusion::arrow::datatypes::IntervalYearMonthType>();
                                arr.value(row_idx) as i64
                            }
                            IntervalUnit::DayTime => {
                                let arr = array.as_primitive::<datafusion::arrow::datatypes::IntervalDayTimeType>();
                                let val = arr.value(row_idx);
                                val.days as i64 * 86_400_000 + val.milliseconds as i64
                            }
                            IntervalUnit::MonthDayNano => {
                                let arr = array.as_primitive::<datafusion::arrow::datatypes::IntervalMonthDayNanoType>();
                                let val = arr.value(row_idx);
                                val.months as i64 * 2_592_000_000_000_000
                                    + val.days as i64 * 86_400_000_000_000
                                    + val.nanoseconds
                            }
                        }
                    }
                    DataType::Duration(unit) => {
                        use datafusion::arrow::datatypes::TimeUnit;
                        match unit {
                            TimeUnit::Second => {
                                let arr = array.as_primitive::<datafusion::arrow::datatypes::DurationSecondType>();
                                arr.value(row_idx)
                            }
                            TimeUnit::Millisecond => {
                                let arr = array.as_primitive::<datafusion::arrow::datatypes::DurationMillisecondType>();
                                arr.value(row_idx)
                            }
                            TimeUnit::Microsecond => {
                                let arr = array.as_primitive::<datafusion::arrow::datatypes::DurationMicrosecondType>();
                                arr.value(row_idx)
                            }
                            TimeUnit::Nanosecond => {
                                let arr = array.as_primitive::<datafusion::arrow::datatypes::DurationNanosecondType>();
                                arr.value(row_idx)
                            }
                        }
                    }
                    _ => {
                        return Err(DataFusionError::Execution(format!(
                            "Unexpected data type for interval percentile: {:?}",
                            self.data_type
                        )));
                    }
                };

                if let Some(count) = self.value_counts.get_mut(&value_i64) {
                    *count -= 1;
                    self.total_count -= 1;

                    if *count == 0 {
                        self.value_counts.remove(&value_i64);
                    }
                }
            }
        }

        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }
}

/// Extract an array of f64 percentile values from a PhysicalExpr
fn extract_percentiles_array(expr: &Arc<dyn PhysicalExpr>) -> Result<Vec<f64>, DataFusionError> {
    // Create empty batch to evaluate the physical expression
    let fields: Vec<Field> = vec![];
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new_with_options(
        schema,
        vec![],
        &RecordBatchOptions::default().with_row_count(Some(1)),
    )?;

    // Evaluate the expression to get a scalar value
    let col_val = expr.evaluate(&batch).map_err(|e| {
        DataFusionError::Execution(format!("Failed to evaluate percentile expression: {e}"))
    })?;

    let scalar = match col_val {
        ColumnarValue::Scalar(s) => s,
        ColumnarValue::Array(arr) => {
            if arr.is_empty() {
                return Err(DataFusionError::Execution(
                    "Cannot extract percentile from empty array".into(),
                ));
            }
            ScalarValue::try_from_array(arr.as_ref(), 0)?
        }
    };

    // Extract array of percentiles
    match scalar {
        ScalarValue::List(arr) => {
            if arr.is_empty() {
                return Err(DataFusionError::Execution(
                    "Percentiles array cannot be empty".into(),
                ));
            }

            // Get the values array from the ListArray
            let values = arr.values();

            // Use values.len() instead of arr.len() to get the number of elements in the list
            let mut percentiles = Vec::with_capacity(values.len());
            for i in 0..values.len() {
                // Check if this element is null in the values array
                if values.is_null(i) {
                    return Err(DataFusionError::Execution(format!(
                        "Percentiles array element at index {i} is NULL"
                    )));
                }

                // Extract the scalar value directly from the values array
                let val_scalar = ScalarValue::try_from_array(values.as_ref(), i).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to extract array element at index {i}: {e}"
                    ))
                })?;

                let percentile = scalar_to_f64(&val_scalar).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to convert array element at index {i} (value: {val_scalar:?}) to f64: {e}"
                    ))
                })?;
                percentiles.push(percentile);
            }
            Ok(percentiles)
        }
        other => Err(DataFusionError::Execution(format!(
            "Expected List type for percentiles array, got: {other:?}"
        ))),
    }
}

/// Convert a ScalarValue to f64 (helper for percentile extraction)
fn scalar_to_f64(scalar: &ScalarValue) -> Result<f64, DataFusionError> {
    let percentile: f64 = match scalar {
        ScalarValue::Float64(Some(v)) => *v,
        ScalarValue::Float32(Some(v)) => *v as f64,
        ScalarValue::Float16(Some(v)) => f64::from(*v),
        ScalarValue::Int8(_)
        | ScalarValue::Int16(_)
        | ScalarValue::Int32(_)
        | ScalarValue::Int64(_)
        | ScalarValue::UInt8(_)
        | ScalarValue::UInt16(_)
        | ScalarValue::UInt32(_)
        | ScalarValue::UInt64(_) => {
            let int_val = LiteralValue(scalar).try_to_i64().map_err(|e| {
                DataFusionError::Execution(format!(
                    "Cannot convert percentile literal {:?} to integer: {}",
                    scalar, e
                ))
            })?;
            int_val as f64
        }
        ScalarValue::Decimal128(Some(v), _precision, scale) => {
            (*v as f64) / 10f64.powi(*scale as i32)
        }
        _ => {
            return Err(DataFusionError::Execution(format!(
                "Cannot convert percentile literal {:?} to f64",
                scalar
            )));
        }
    };
    Ok(percentile)
}

/// Extract a literal f64 value from a PhysicalExpr (for percentile parameter)
fn extract_literal(expr: &Arc<dyn PhysicalExpr>) -> Result<f64, DataFusionError> {
    // Create empty batch to evaluate the physical expression
    let fields: Vec<Field> = vec![];
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new_with_options(
        schema,
        vec![],
        &RecordBatchOptions::default().with_row_count(Some(1)),
    )?;

    // Evaluate the expression to get a scalar value
    let col_val = expr.evaluate(&batch)?;
    let scalar = match col_val {
        ColumnarValue::Scalar(s) => s,
        ColumnarValue::Array(arr) => ScalarValue::try_from_array(arr.as_ref(), 0)?,
    };

    // Check if it's a List first - this should be handled by extract_percentiles_array
    if matches!(scalar, ScalarValue::List(_)) {
        return Err(DataFusionError::Execution(
            "Array of percentiles detected but not properly handled. Use array syntax: percentile(col, array(0.25, 0.5, 0.75))".into()
        ));
    }

    // Use helper function to convert to f64
    scalar_to_f64(&scalar)
}

#[cfg(test)]
mod numeric_frequency_tests {
    use super::*;
    use datafusion::arrow::array::{Float64Array, Int64Array};

    fn inputs(values: Vec<Option<f64>>, frequencies: Vec<Option<i64>>) -> Vec<ArrayRef> {
        let probabilities = Arc::new(Float64Array::from(vec![0.5; values.len()]));
        vec![
            Arc::new(Float64Array::from(values)),
            probabilities,
            Arc::new(Int64Array::from(frequencies)),
        ]
    }

    #[test]
    fn weighted_state_merges_and_retracts_counts() -> Result<()> {
        let mut first = NumericPercentileAccumulator::new(0.25);
        let first_rows = inputs(vec![Some(1.), Some(9.)], vec![Some(2), Some(2)]);
        first.update_batch(&first_rows)?;
        assert_eq!(first.evaluate()?, ScalarValue::Float64(Some(1.)));
        let mut second = NumericPercentileAccumulator::new(0.25);
        let second_rows = inputs(
            vec![Some(9.), Some(100.), Some(-100.), None],
            vec![Some(3), Some(0), None, Some(10)],
        );
        second.update_batch(&second_rows)?;
        let states = second
            .state()?
            .iter()
            .map(|state| state.to_array())
            .collect::<Result<Vec<_>>>()?;
        first.merge_batch(&states)?;
        assert_eq!(first.evaluate()?, ScalarValue::Float64(Some(5.)));
        first.retract_batch(&first_rows)?;
        assert_eq!(first.total_count, 3);
        assert_eq!(first.value_counts.len(), 1);
        assert_eq!(first.evaluate()?, ScalarValue::Float64(Some(9.)));
        first.retract_batch(&second_rows)?;
        assert_eq!(first.evaluate()?, ScalarValue::Float64(None));
        Ok(())
    }

    #[test]
    fn large_weights_keep_distinct_state_and_account_for_retained_memory() -> Result<()> {
        let mut weighted = NumericPercentileAccumulator::with_percentiles(vec![0., 0.5, 1.], true);
        weighted.update_batch(&inputs(
            vec![Some(1.), Some(9.)],
            vec![Some(1_000_000_000_000), Some(1)],
        ))?;
        let mut ordinary = NumericPercentileAccumulator::with_percentiles(vec![0., 0.5, 1.], true);
        ordinary.update_batch(&inputs(vec![Some(1.), Some(9.)], vec![Some(1), Some(1)]))?;
        assert_eq!(weighted.size(), ordinary.size());
        let state = weighted.state()?;
        assert_eq!(state.len(), 2);
        let ScalarValue::List(values) = &state[0] else {
            return Err(DataFusionError::Internal("Expected values list".into()));
        };
        let ScalarValue::List(counts) = &state[1] else {
            return Err(DataFusionError::Internal("Expected counts list".into()));
        };
        assert_eq!(values.value(0).len(), 2);
        assert_eq!(counts.value(0).len(), 2);
        let counts_array = counts.value(0);
        assert_eq!(
            as_uint64_array(&counts_array)?.values().iter().sum::<u64>(),
            1_000_000_000_001
        );
        let mut merged = NumericPercentileAccumulator::with_percentiles(vec![0., 0.5, 1.], true);
        merged.merge_batch(
            &state
                .iter()
                .map(|state| state.to_array())
                .collect::<Result<Vec<_>>>()?,
        )?;
        assert_eq!(
            merged.evaluate()?,
            ScalarValue::List(ScalarValue::new_list_nullable(
                &[
                    ScalarValue::Float64(Some(1.)),
                    ScalarValue::Float64(Some(1.)),
                    ScalarValue::Float64(Some(9.))
                ],
                &DataType::Float64
            ))
        );
        Ok(())
    }

    #[test]
    fn count_overflow_and_retraction_underflow_do_not_corrupt_state() -> Result<()> {
        let mut accumulator = NumericPercentileAccumulator::new(0.5);
        accumulator.apply_count(1., u64::MAX, false)?;
        assert!(accumulator.apply_count(9., 1, false).is_err());
        assert_eq!(accumulator.total_count, u64::MAX);
        assert_eq!(accumulator.value_counts.len(), 1);
        accumulator.apply_count(1., u64::MAX, true)?;
        accumulator.apply_count(1., 1, false)?;
        assert!(accumulator.apply_count(1., 2, true).is_err());
        assert_eq!(accumulator.total_count, 1);
        assert_eq!(accumulator.evaluate()?, ScalarValue::Float64(Some(1.)));
        Ok(())
    }
}
