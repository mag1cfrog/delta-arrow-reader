//! Spark floating extrema compare normalized keys and retain the selected input bits.

use std::cmp::Ordering;
use std::mem::{size_of, size_of_val};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, ArrowNativeTypeOp, AsArray, BooleanArray, BooleanBufferBuilder, PrimitiveArray,
};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{ArrowPrimitiveType, DataType, Float32Type, Float64Type};
use datafusion::functions_aggregate::min_max::{self, MovingMin};
use datafusion::physical_expr::expressions::Column;
use datafusion_common::stats::Precision;
use datafusion_common::{DFSchema, Result, ScalarValue, exec_err, plan_err};
use datafusion_expr::function::AccumulatorArgs;
use datafusion_expr::utils::AggregateOrderSensitivity;
use datafusion_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, EmitTo, Expr, ExprSchemable, GroupsAccumulator,
    Signature, StatisticsArgs, Volatility,
};

use super::common::{normalize_spark_float32, normalize_spark_float64};

fn float_type(data_type: &DataType) -> Option<&DataType> {
    match data_type {
        DataType::Float32 | DataType::Float64 => Some(data_type),
        DataType::Dictionary(_, value) => float_type(value),
        _ => None,
    }
}

pub(crate) fn extrema_udaf(
    args: &[Expr],
    schema: &DFSchema,
    is_max: bool,
) -> Result<Arc<AggregateUDF>> {
    if let [argument] = args
        && float_type(&argument.get_type(schema)?).is_some()
    {
        return Ok(Arc::new(AggregateUDF::from(SparkFloatExtrema {
            is_max,
            signature: Signature::user_defined(Volatility::Immutable),
        })));
    }
    Ok(if is_max {
        min_max::max_udaf()
    } else {
        min_max::min_udaf()
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct SparkFloatExtrema {
    is_max: bool,
    signature: Signature,
}

impl AggregateUDFImpl for SparkFloatExtrema {
    fn name(&self) -> &str {
        if self.is_max { "max" } else { "min" }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, args: &[DataType]) -> Result<Vec<DataType>> {
        if let [arg] = args
            && let Some(data_type) = float_type(arg)
        {
            return Ok(vec![data_type.clone()]);
        }
        plan_err!("{} requires one FLOAT or DOUBLE argument", self.name())
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        Ok(self.coerce_types(args)?[0].clone())
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(match args.return_type() {
            DataType::Float32 => Box::new(FloatAccumulator::<Float32Type> {
                value: None,
                is_max: self.is_max,
            }),
            DataType::Float64 => Box::new(FloatAccumulator::<Float64Type> {
                value: None,
                is_max: self.is_max,
            }),
            other => return plan_err!("unexpected Spark extrema type {other}"),
        })
    }

    fn groups_accumulator_supported(&self, _: AccumulatorArgs) -> bool {
        true
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        Ok(match args.return_type() {
            DataType::Float32 => Box::new(FloatGroupsAccumulator::<Float32Type>::new(self.is_max)),
            DataType::Float64 => Box::new(FloatGroupsAccumulator::<Float64Type>::new(self.is_max)),
            other => return plan_err!("unexpected Spark extrema type {other}"),
        })
    }

    fn create_sliding_accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(match args.return_type() {
            DataType::Float32 => Box::new(FloatSlidingAccumulator::<Float32Type>::new(self.is_max)),
            DataType::Float64 => Box::new(FloatSlidingAccumulator::<Float64Type>::new(self.is_max)),
            other => return plan_err!("unexpected Spark extrema type {other}"),
        })
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        AggregateOrderSensitivity::Insensitive
    }

    fn value_from_stats(&self, args: &StatisticsArgs) -> Option<ScalarValue> {
        let Precision::Exact(rows) = args.statistics.num_rows else {
            return None;
        };
        if rows == 0 {
            return ScalarValue::try_from(args.return_type).ok();
        }
        let [expression] = args.exprs else {
            return None;
        };
        let column = expression.downcast_ref::<Column>()?;
        let stats = args.statistics.column_statistics.get(column.index())?;
        if stats.null_count == Precision::Exact(rows) {
            return ScalarValue::try_from(args.return_type).ok();
        }
        // A numeric minimum survives hidden NaNs; a numeric maximum needs to exclude them.
        if self.is_max && stats.null_count != Precision::Exact(rows - 1) {
            return None;
        }
        let native = if self.is_max {
            min_max::max_udaf()
        } else {
            min_max::min_udaf()
        };
        let value = native.value_from_stats(args)?;
        match value {
            ScalarValue::Float32(Some(v)) if v != 0.0 && !v.is_nan() => Some(value),
            ScalarValue::Float64(Some(v)) if v != 0.0 && !v.is_nan() => Some(value),
            _ => None,
        }
    }
}

trait SparkFloat: ArrowPrimitiveType + std::fmt::Debug {
    fn normalize(value: Self::Native) -> Self::Native;
    fn scalar(value: Option<Self::Native>) -> ScalarValue;
}

impl SparkFloat for Float32Type {
    fn normalize(value: f32) -> f32 {
        normalize_spark_float32(value)
    }
    fn scalar(value: Option<f32>) -> ScalarValue {
        ScalarValue::Float32(value)
    }
}

impl SparkFloat for Float64Type {
    fn normalize(value: f64) -> f64 {
        normalize_spark_float64(value)
    }
    fn scalar(value: Option<f64>) -> ScalarValue {
        ScalarValue::Float64(value)
    }
}

fn strictly_better<T: SparkFloat>(new: T::Native, current: T::Native, is_max: bool) -> bool {
    T::normalize(new).compare(T::normalize(current))
        == if is_max {
            Ordering::Greater
        } else {
            Ordering::Less
        }
}

#[derive(Debug)]
struct FloatAccumulator<T: SparkFloat> {
    value: Option<T::Native>,
    is_max: bool,
}

impl<T: SparkFloat> Accumulator for FloatAccumulator<T> {
    fn update_batch(&mut self, arrays: &[ArrayRef]) -> Result<()> {
        for value in arrays[0].as_primitive::<T>().iter().flatten() {
            if self
                .value
                .is_none_or(|old| strictly_better::<T>(value, old, self.is_max))
            {
                self.value = Some(value);
            }
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![T::scalar(self.value)])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(T::scalar(self.value))
    }

    fn size(&self) -> usize {
        size_of_val(self)
    }
}

#[derive(Debug)]
struct FloatGroupsAccumulator<T: SparkFloat> {
    values: Vec<T::Native>,
    has_value: BooleanBufferBuilder,
    is_max: bool,
}

impl<T: SparkFloat> FloatGroupsAccumulator<T> {
    fn new(is_max: bool) -> Self {
        Self {
            values: Vec::new(),
            has_value: BooleanBufferBuilder::new(0),
            is_max,
        }
    }
}

impl<T: SparkFloat> GroupsAccumulator for FloatGroupsAccumulator<T> {
    fn update_batch(
        &mut self,
        arrays: &[ArrayRef],
        groups: &[usize],
        filter: Option<&BooleanArray>,
        total_groups: usize,
    ) -> Result<()> {
        let array = arrays[0].as_primitive::<T>();
        self.values.resize(total_groups, T::default_value());
        self.has_value.resize(total_groups);
        for (row, (&group, value)) in groups.iter().zip(array.iter()).enumerate() {
            if filter.is_some_and(|filter| filter.is_null(row) || !filter.value(row)) {
                continue;
            }
            if let Some(value) = value
                && (!self.has_value.get_bit(group)
                    || strictly_better::<T>(value, self.values[group], self.is_max))
            {
                self.values[group] = value;
                self.has_value.set_bit(group, true);
            }
        }
        Ok(())
    }

    fn merge_batch(
        &mut self,
        arrays: &[ArrayRef],
        groups: &[usize],
        filter: Option<&BooleanArray>,
        total_groups: usize,
    ) -> Result<()> {
        self.update_batch(arrays, groups, filter, total_groups)
    }

    fn evaluate(&mut self, emit: EmitTo) -> Result<ArrayRef> {
        let values = emit.take_needed(&mut self.values);
        let seen = self.has_value.finish();
        let nulls = seen.slice(0, values.len());
        self.has_value
            .append_buffer(&seen.slice(values.len(), self.values.len()));
        Ok(Arc::new(PrimitiveArray::<T>::new(
            values.into(),
            Some(NullBuffer::new(nulls)),
        )))
    }

    fn state(&mut self, emit: EmitTo) -> Result<Vec<ArrayRef>> {
        Ok(vec![self.evaluate(emit)?])
    }

    fn convert_to_state(
        &self,
        arrays: &[ArrayRef],
        filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        let Some(filter) = filter else {
            return Ok(vec![arrays[0].clone()]);
        };
        let array = arrays[0].as_primitive::<T>();
        let nulls = NullBuffer::from_iter(
            (0..array.len())
                .map(|row| array.is_valid(row) && filter.is_valid(row) && filter.value(row)),
        );
        Ok(vec![Arc::new(PrimitiveArray::<T>::new(
            array.values().clone(),
            Some(nulls),
        ))])
    }

    fn supports_convert_to_state(&self) -> bool {
        true
    }

    fn size(&self) -> usize {
        size_of_val(self)
            + self.values.capacity() * size_of::<T::Native>()
            + self.has_value.capacity().div_ceil(8)
    }
}

#[derive(Debug, Clone)]
struct WindowValue<F: ArrowNativeTypeOp> {
    raw: F,
    key: F,
    arrival: u64,
    is_max: bool,
}

impl<F: ArrowNativeTypeOp> PartialEq for WindowValue<F> {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other) == Some(Ordering::Equal)
    }
}

impl<F: ArrowNativeTypeOp> PartialOrd for WindowValue<F> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let order = self.key.compare(other.key);
        Some(
            (if self.is_max { order.reverse() } else { order })
                .then_with(|| self.arrival.cmp(&other.arrival)),
        )
    }
}

#[derive(Debug)]
struct FloatSlidingAccumulator<T: SparkFloat> {
    queue: MovingMin<WindowValue<T::Native>>,
    next_arrival: u64,
    is_max: bool,
}

impl<T: SparkFloat> FloatSlidingAccumulator<T> {
    fn new(is_max: bool) -> Self {
        Self {
            queue: MovingMin::new(),
            next_arrival: 0,
            is_max,
        }
    }
}

impl<T: SparkFloat> Accumulator for FloatSlidingAccumulator<T> {
    fn update_batch(&mut self, arrays: &[ArrayRef]) -> Result<()> {
        for value in arrays[0].as_primitive::<T>().iter().flatten() {
            let arrival = self.next_arrival;
            self.next_arrival = arrival.checked_add(1).ok_or_else(|| {
                datafusion_common::DataFusionError::Execution(
                    "floating window arrival counter overflow".into(),
                )
            })?;
            self.queue.push(WindowValue {
                raw: value,
                key: T::normalize(value),
                arrival,
                is_max: self.is_max,
            });
        }
        Ok(())
    }

    fn retract_batch(&mut self, arrays: &[ArrayRef]) -> Result<()> {
        for _ in arrays[0].as_primitive::<T>().iter().flatten() {
            if self.queue.pop().is_none() {
                return exec_err!("floating window retracted more values than were admitted");
            }
        }
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.update_batch(states)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(T::scalar(self.queue.min().map(|value| value.raw)))
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.queue.buffer_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw_bits(value: ScalarValue) -> Option<u64> {
        match value {
            ScalarValue::Float32(value) => value.map(|value| u64::from(value.to_bits())),
            ScalarValue::Float64(value) => value.map(f64::to_bits),
            other => unreachable!("expected floating scalar, got {other:?}"),
        }
    }

    fn array<T: SparkFloat>(values: impl IntoIterator<Item = Option<T::Native>>) -> ArrayRef {
        Arc::new(PrimitiveArray::<T>::from_iter(values))
    }

    fn assert_value<T: SparkFloat>(actual: ScalarValue, expected: Option<T::Native>) {
        assert_eq!(raw_bits(actual), raw_bits(T::scalar(expected)));
    }

    fn check_width<T: SparkFloat>(
        negative_zero: T::Native,
        positive_zero: T::Native,
        nan_a: T::Native,
        nan_b: T::Native,
        negative_inf: T::Native,
        positive_inf: T::Native,
    ) -> Result<()> {
        for is_max in [false, true] {
            let mut scalar = FloatAccumulator::<T> {
                value: None,
                is_max,
            };
            scalar.update_batch(&[array::<T>([None])])?;
            assert_value::<T>(scalar.evaluate()?, None);
            scalar.update_batch(&[array::<T>([Some(negative_zero), None, Some(positive_zero)])])?;
            scalar.merge_batch(&[array::<T>([Some(positive_zero)])])?;
            assert_value::<T>(scalar.evaluate()?, Some(negative_zero));
            let mut opposite = FloatAccumulator::<T> {
                value: None,
                is_max,
            };
            opposite.merge_batch(&[array::<T>([Some(positive_zero), Some(negative_zero)])])?;
            assert_value::<T>(opposite.evaluate()?, Some(positive_zero));

            let mut nan = FloatAccumulator::<T> {
                value: None,
                is_max,
            };
            nan.update_batch(&[array::<T>([Some(nan_a), None, Some(nan_b)])])?;
            nan.merge_batch(&[array::<T>([Some(nan_b)])])?;
            assert_value::<T>(nan.evaluate()?, Some(nan_a));
            nan.update_batch(&[array::<T>([Some(positive_inf), Some(negative_inf)])])?;
            assert_value::<T>(
                nan.evaluate()?,
                Some(if is_max { nan_a } else { negative_inf }),
            );

            let mut window = FloatSlidingAccumulator::<T>::new(is_max);
            assert!(window.supports_retract_batch());
            window.update_batch(&[array::<T>([
                Some(negative_zero),
                None,
                Some(positive_zero),
                Some(nan_a),
                Some(nan_b),
            ])])?;
            assert_value::<T>(
                window.evaluate()?,
                Some(if is_max { nan_a } else { negative_zero }),
            );
            window.retract_batch(&[array::<T>([Some(negative_zero), None])])?;
            assert_value::<T>(
                window.evaluate()?,
                Some(if is_max { nan_a } else { positive_zero }),
            );
            window.update_batch(&[array::<T>([Some(negative_zero)])])?;
            assert_value::<T>(
                window.evaluate()?,
                Some(if is_max { nan_a } else { positive_zero }),
            );
            window.retract_batch(&[array::<T>([Some(positive_zero), Some(nan_a)])])?;
            assert_value::<T>(
                window.evaluate()?,
                Some(if is_max { nan_b } else { negative_zero }),
            );
            window.retract_batch(&[array::<T>([Some(nan_b)])])?;
            assert_value::<T>(window.evaluate()?, Some(negative_zero));
            window.retract_batch(&[array::<T>([Some(negative_zero)])])?;
            assert_value::<T>(window.evaluate()?, None);
            assert!(
                window
                    .retract_batch(&[array::<T>([Some(positive_zero)])])
                    .is_err()
            );
            window.next_arrival = u64::MAX;
            assert!(
                window
                    .update_batch(&[array::<T>([Some(positive_zero)])])
                    .is_err()
            );
            assert_value::<T>(window.evaluate()?, None);

            let mut grouped = FloatGroupsAccumulator::<T>::new(is_max);
            let values = array::<T>([
                Some(nan_a),
                Some(nan_b),
                Some(negative_zero),
                Some(positive_zero),
                Some(negative_inf),
                Some(positive_inf),
                Some(nan_b),
                None,
            ]);
            let filter = BooleanArray::from(vec![
                Some(true),
                Some(true),
                Some(true),
                Some(true),
                Some(true),
                Some(true),
                None,
                Some(true),
            ]);
            grouped.update_batch(
                &[values.clone()],
                &[0, 0, 1, 1, 2, 3, 4, 4],
                Some(&filter),
                6,
            )?;
            assert_eq!(
                grouped.size(),
                size_of_val(&grouped)
                    + grouped.values.capacity() * size_of::<T::Native>()
                    + grouped.has_value.capacity().div_ceil(8)
            );
            let first = grouped.evaluate(EmitTo::First(2))?;
            assert_value::<T>(ScalarValue::try_from_array(&first, 0)?, Some(nan_a));
            assert_value::<T>(ScalarValue::try_from_array(&first, 1)?, Some(negative_zero));
            grouped.merge_batch(&[array::<T>([Some(nan_a), Some(nan_b)])], &[0, 0], None, 4)?;
            let rest = grouped.state(EmitTo::All)?;
            assert_value::<T>(
                ScalarValue::try_from_array(&rest[0], 0)?,
                Some(if is_max { nan_a } else { negative_inf }),
            );
            assert_value::<T>(
                ScalarValue::try_from_array(&rest[0], 1)?,
                Some(positive_inf),
            );
            assert_value::<T>(ScalarValue::try_from_array(&rest[0], 2)?, None);
            assert_value::<T>(ScalarValue::try_from_array(&rest[0], 3)?, None);
            let unchanged = grouped.convert_to_state(&[values.clone()], None)?;
            assert!(Arc::ptr_eq(&values, &unchanged[0]));
            let filtered = grouped.convert_to_state(&[values], Some(&filter))?;
            assert_value::<T>(ScalarValue::try_from_array(&filtered[0], 0)?, Some(nan_a));
            assert!(filtered[0].is_null(6));
            assert!(filtered[0].is_null(7));
        }
        Ok(())
    }

    #[test]
    fn raw_extrema_preserve_ties_across_updates_merges_groups_and_retraction() -> Result<()> {
        check_width::<Float32Type>(
            -0.0,
            0.0,
            f32::from_bits(0x7fc01234),
            f32::from_bits(0xffc04321),
            f32::NEG_INFINITY,
            f32::INFINITY,
        )?;
        check_width::<Float64Type>(
            -0.0,
            0.0,
            f64::from_bits(0x7ff8000000001234),
            f64::from_bits(0xfff8000000004321),
            f64::NEG_INFINITY,
            f64::INFINITY,
        )
    }

    #[test]
    fn sliding_memory_counts_retained_queue_buffers_after_transfer_and_drain() -> Result<()> {
        let mut zero_sized = MovingMin::<()>::new();
        zero_sized.push(());
        assert_eq!(zero_sized.buffer_bytes(), 0);
        assert_eq!(zero_sized.pop(), Some(()));
        assert_eq!(zero_sized.buffer_bytes(), 0);
        let mut window = FloatSlidingAccumulator::<Float64Type>::new(false);
        let inline = window.size();
        let values = array::<Float64Type>((0..1024).map(|i| Some(f64::from(i))));
        window.update_batch(&[values.clone()])?;
        let grown = window.size();
        assert!(grown > inline);
        window.retract_batch(&[values.slice(0, 1)])?;
        let transferred = window.size();
        assert!(transferred > grown);
        window.retract_batch(&[values.slice(1, values.len() - 1)])?;
        assert_value::<Float64Type>(window.evaluate()?, None);
        assert_eq!(window.size(), transferred);
        Ok(())
    }

    #[test]
    fn statistics_preserve_numeric_minima_and_require_no_nan_proofs_for_maxima() -> Result<()> {
        use arrow::datatypes::{Field, Schema};
        use datafusion::physical_expr::PhysicalExpr;
        use datafusion_common::{ColumnStatistics, Statistics};

        for data_type in [DataType::Float32, DataType::Float64] {
            let schema = Schema::new(vec![Field::new("k", data_type.clone(), true)]);
            let expressions: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::new(Column::new("k", 0))];
            let ordinary = match data_type {
                DataType::Float32 => ScalarValue::Float32(Some(1.5)),
                _ => ScalarValue::Float64(Some(1.5)),
            };
            let mut stats = Statistics::new_unknown(&schema);
            stats.num_rows = Precision::Exact(100);
            stats.column_statistics = vec![ColumnStatistics::new_unknown()];
            stats.column_statistics[0].null_count = Precision::Exact(0);
            stats.column_statistics[0].min_value = Precision::Exact(ordinary.clone());
            stats.column_statistics[0].max_value = Precision::Exact(ordinary.clone());
            for is_max in [false, true] {
                let function = SparkFloatExtrema {
                    is_max,
                    signature: Signature::user_defined(Volatility::Immutable),
                };
                let evaluate = |stats: &Statistics| {
                    function.value_from_stats(&StatisticsArgs {
                        statistics: stats,
                        return_type: &data_type,
                        is_distinct: false,
                        exprs: &expressions,
                    })
                };
                assert_eq!(evaluate(&stats), (!is_max).then(|| ordinary.clone()));
                stats.column_statistics[0].null_count = Precision::Absent;
                assert_eq!(evaluate(&stats), (!is_max).then(|| ordinary.clone()));
                stats.column_statistics[0].null_count = Precision::Exact(99);
                assert_eq!(evaluate(&stats), Some(ordinary.clone()));
                stats.num_rows = Precision::Inexact(100);
                assert_eq!(evaluate(&stats), None);
                stats.num_rows = Precision::Exact(100);
                stats.column_statistics[0].null_count = Precision::Exact(100);
                assert_eq!(evaluate(&stats), Some(ScalarValue::try_from(&data_type)?));
                stats.num_rows = Precision::Exact(0);
                assert_eq!(evaluate(&stats), Some(ScalarValue::try_from(&data_type)?));
                stats.num_rows = Precision::Exact(1);
                stats.column_statistics[0].null_count = Precision::Exact(0);
                for boundary in [0.0, -0.0, f64::NAN] {
                    let value = match data_type {
                        DataType::Float32 => ScalarValue::Float32(Some(boundary as f32)),
                        _ => ScalarValue::Float64(Some(boundary)),
                    };
                    stats.column_statistics[0].min_value = Precision::Exact(value.clone());
                    stats.column_statistics[0].max_value = Precision::Exact(value);
                    assert_eq!(evaluate(&stats), None);
                }
                stats.num_rows = Precision::Exact(100);
                stats.column_statistics[0].null_count = Precision::Exact(0);
                stats.column_statistics[0].min_value = Precision::Exact(ordinary.clone());
                stats.column_statistics[0].max_value = Precision::Exact(ordinary.clone());
            }
        }
        Ok(())
    }
}
