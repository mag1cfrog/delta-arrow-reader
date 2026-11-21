// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::ops::Deref;

/// [Credit]: <https://github.com/datafusion-contrib/datafusion-functions-extra/blob/5fa184df2589f09e90035c5e6a0d2c88c57c298a/src/max_min_by.rs>
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::common::ScalarValue;
use datafusion::common::cast::as_int64_array;
use datafusion::error::DataFusionError;
use datafusion::functions_aggregate::first_last::last_value_udaf;
use datafusion::logical_expr::expr::{AggregateFunction, Sort};
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::simplify::SimplifyContext;
use datafusion::logical_expr::utils::format_state_name;
use datafusion::logical_expr::{Accumulator, AggregateUDFImpl, Signature, Volatility, function};
use datafusion::prelude::Expr;

#[derive(Debug)]
struct MaxMinByAccumulator {
    value: ScalarValue,
    ordering: ScalarValue,
    is_max: bool,
}

impl MaxMinByAccumulator {
    fn new(
        value_type: &DataType,
        ordering_type: &DataType,
        is_max: bool,
    ) -> Result<Self, DataFusionError> {
        Ok(Self {
            value: ScalarValue::try_from(value_type)?,
            ordering: ScalarValue::try_from(ordering_type)?,
            is_max,
        })
    }
}

impl Accumulator for MaxMinByAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<(), DataFusionError> {
        let value_array = &values[0];
        let ordering_array = &values[1];

        for i in 0..ordering_array.len() {
            if ordering_array.is_null(i) {
                continue;
            }
            let ordering_val = ScalarValue::try_from_array(&values[1], i)?;
            let should_update = if self.ordering.is_null() {
                true
            } else {
                match ordering_val.partial_cmp(&self.ordering) {
                    Some(Ordering::Greater) => self.is_max,
                    Some(Ordering::Less) => !self.is_max,
                    _ => false,
                }
            };
            if should_update {
                self.value = ScalarValue::try_from_array(value_array, i)?;
                self.ordering = ordering_val;
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue, DataFusionError> {
        Ok(self.value.clone())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>, DataFusionError> {
        Ok(vec![self.value.clone(), self.ordering.clone()])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<(), DataFusionError> {
        self.update_batch(states)
    }

    fn size(&self) -> usize {
        self.value.size() + self.ordering.size() + std::mem::size_of::<bool>()
    }
}

#[derive(Debug)]
struct Int64ExtremumRow {
    position: usize,
    key: i64,
    value: Option<i64>,
}

#[derive(Debug)]
struct SlidingInt64ByKeyAccumulator {
    candidates: VecDeque<Int64ExtremumRow>,
    rows_added: usize,
    rows_removed: usize,
    is_max: bool,
}

impl SlidingInt64ByKeyAccumulator {
    fn new(is_max: bool) -> Self {
        Self {
            candidates: VecDeque::new(),
            rows_added: 0,
            rows_removed: 0,
            is_max,
        }
    }
}

impl Accumulator for SlidingInt64ByKeyAccumulator {
    fn update_batch(&mut self, arrays: &[ArrayRef]) -> Result<(), DataFusionError> {
        let values = as_int64_array(&arrays[0])?;
        let keys = as_int64_array(&arrays[1])?;
        let better_ordering = if self.is_max {
            Ordering::Greater
        } else {
            Ordering::Less
        };
        for (offset, (key, value)) in keys.iter().zip(values.iter()).enumerate() {
            let Some(key) = key else { continue };
            // Keep equal keys in arrival order. The earliest row wins until it leaves.
            while self
                .candidates
                .back()
                .is_some_and(|row| key.cmp(&row.key) == better_ordering)
            {
                self.candidates.pop_back();
            }
            self.candidates.push_back(Int64ExtremumRow {
                position: self.rows_added + offset,
                key,
                value,
            });
        }
        self.rows_added += keys.len();
        Ok(())
    }

    fn retract_batch(&mut self, arrays: &[ArrayRef]) -> Result<(), DataFusionError> {
        self.rows_removed += arrays[1].len();
        while self
            .candidates
            .front()
            .is_some_and(|row| row.position < self.rows_removed)
        {
            self.candidates.pop_front();
        }
        Ok(())
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }

    fn evaluate(&mut self) -> Result<ScalarValue, DataFusionError> {
        Ok(ScalarValue::Int64(
            self.candidates.front().and_then(|row| row.value),
        ))
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>, DataFusionError> {
        let value = self.evaluate()?;
        let key = self.candidates.front().map(|row| row.key);
        Ok(vec![value, ScalarValue::Int64(key)])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<(), DataFusionError> {
        self.update_batch(states)
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self.candidates.capacity() * std::mem::size_of::<Int64ExtremumRow>()
    }
}

fn sliding_by_key_accumulator(
    args: AccumulatorArgs,
    is_max: bool,
) -> Result<Box<dyn Accumulator>, DataFusionError> {
    let value_type = args.return_field.data_type();
    let key_type = args.exprs[1].data_type(args.schema)?;
    if value_type == &DataType::Int64 && key_type == DataType::Int64 {
        return Ok(Box::new(SlidingInt64ByKeyAccumulator::new(is_max)));
    }
    Ok(Box::new(MaxMinByAccumulator::new(
        value_type, &key_type, is_max,
    )?))
}

#[derive(PartialEq, Eq, Hash)]
pub struct MaxByFunction {
    signature: Signature,
}

impl Debug for MaxByFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("MaxBy")
            .field("name", &self.name())
            .field("signature", &self.signature)
            .field("accumulator", &"<FUNC>")
            .finish()
    }
}
impl Default for MaxByFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl MaxByFunction {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

fn get_min_max_by_result_type(input_types: &[DataType]) -> Result<Vec<DataType>, DataFusionError> {
    match &input_types[0] {
        DataType::Dictionary(_, dict_value_type) => {
            // TODO add checker, if the value type is complex data type
            Ok(vec![dict_value_type.deref().clone()])
        }
        _ => Ok(input_types.to_vec()),
    }
}

impl AggregateUDFImpl for MaxByFunction {
    fn name(&self) -> &str {
        "max_by"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        Ok(arg_types[0].to_owned())
    }

    fn accumulator(
        &self,
        acc_args: AccumulatorArgs,
    ) -> Result<Box<dyn Accumulator>, DataFusionError> {
        let value_type = acc_args.return_field.data_type().clone();
        let ordering_type = acc_args.exprs[1].data_type(acc_args.schema)?;
        Ok(Box::new(MaxMinByAccumulator::new(
            &value_type,
            &ordering_type,
            true,
        )?))
    }

    fn create_sliding_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn Accumulator>, DataFusionError> {
        sliding_by_key_accumulator(args, true)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>, DataFusionError> {
        let value_type = args.return_field.data_type().clone();
        let ordering_type = args.input_fields[1].data_type().clone();
        Ok(vec![
            Field::new(format_state_name(args.name, "value"), value_type, true).into(),
            Field::new(
                format_state_name(args.name, "ordering"),
                ordering_type,
                true,
            )
            .into(),
        ])
    }

    fn simplify(&self) -> Option<function::AggregateFunctionSimplification> {
        let simplify = |mut aggr_func: AggregateFunction, _: &SimplifyContext| {
            let mut order_by = aggr_func.params.order_by;
            let (second_arg, first_arg) = (
                aggr_func.params.args.remove(1),
                aggr_func.params.args.remove(0),
            );

            let null_filter = second_arg.clone().is_not_null();
            let filter = match aggr_func.params.filter {
                Some(existing) => Some(Box::new((*existing).and(null_filter))),
                None => Some(Box::new(null_filter)),
            };

            order_by.push(Sort::new(second_arg, true, true)); // ASC,  NULLS FIRST

            Ok(Expr::AggregateFunction(AggregateFunction::new_udf(
                last_value_udaf(),
                vec![first_arg],
                aggr_func.params.distinct,
                filter,
                order_by,
                aggr_func.params.null_treatment,
            )))
        };
        Some(Box::new(simplify))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>, DataFusionError> {
        get_min_max_by_result_type(arg_types)
    }
}

#[derive(PartialEq, Eq, Hash)]
pub struct MinByFunction {
    signature: Signature,
}

impl Debug for MinByFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("MinBy")
            .field("name", &self.name())
            .field("signature", &self.signature)
            .field("accumulator", &"<FUNC>")
            .finish()
    }
}

impl Default for MinByFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl MinByFunction {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl AggregateUDFImpl for MinByFunction {
    fn name(&self) -> &str {
        "min_by"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        Ok(arg_types[0].to_owned())
    }

    fn accumulator(
        &self,
        acc_args: AccumulatorArgs,
    ) -> Result<Box<dyn Accumulator>, DataFusionError> {
        let value_type = acc_args.return_field.data_type().clone();
        let ordering_type = acc_args.exprs[1].data_type(acc_args.schema)?;
        Ok(Box::new(MaxMinByAccumulator::new(
            &value_type,
            &ordering_type,
            false,
        )?))
    }

    fn create_sliding_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn Accumulator>, DataFusionError> {
        sliding_by_key_accumulator(args, false)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>, DataFusionError> {
        let value_type = args.return_field.data_type().clone();
        let ordering_type = args.input_fields[1].data_type().clone();
        Ok(vec![
            Field::new(format_state_name(args.name, "value"), value_type, true).into(),
            Field::new(
                format_state_name(args.name, "ordering"),
                ordering_type,
                true,
            )
            .into(),
        ])
    }

    fn simplify(&self) -> Option<function::AggregateFunctionSimplification> {
        let simplify = |mut aggr_func: AggregateFunction, info: &SimplifyContext| {
            let mut order_by = aggr_func.params.order_by;
            let key = aggr_func.params.args.remove(1);
            let value = aggr_func.params.args.remove(0);

            let filter = match aggr_func.params.filter {
                Some(existing) => Some(Box::new((*existing).and(key.clone().is_not_null()))),
                None if matches!(key, Expr::Column(_))
                    && matches!(info.nullable(&key), Ok(false)) =>
                {
                    None
                }
                None => Some(Box::new(key.clone().is_not_null())),
            };

            order_by.push(Sort::new(key, false, true)); // DESC, NULLS FIRST

            Ok(Expr::AggregateFunction(AggregateFunction::new_udf(
                last_value_udaf(),
                vec![value],
                aggr_func.params.distinct,
                filter,
                order_by,
                aggr_func.params.null_treatment,
            )))
        };
        Some(Box::new(simplify))
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>, DataFusionError> {
        get_min_max_by_result_type(arg_types)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::Int64Array;

    use super::*;

    #[test]
    fn sliding_extrema_retain_the_original_state_across_frame_moves() -> Result<(), DataFusionError>
    {
        let values: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(10),
            None,
            Some(12),
            Some(13),
            Some(14),
            Some(15),
            Some(16),
            Some(i64::MIN),
            None,
            Some(i64::MAX),
            Some(20),
            Some(21),
            Some(22),
            Some(23),
        ]));
        let keys: ArrayRef = Arc::new(Int64Array::from(vec![
            None,
            Some(9),
            Some(9),
            Some(i64::MIN),
            Some(i64::MIN),
            Some(-1),
            None,
            Some(i64::MAX),
            Some(i64::MAX),
            Some(0),
            Some(0),
            None,
            Some(2),
            Some(2),
        ]));
        let slice = |start, end| {
            [
                values.slice(start, end - start),
                keys.slice(start, end - start),
            ]
        };
        for is_max in [false, true] {
            let mut sliding = SlidingInt64ByKeyAccumulator::new(is_max);
            let mut last = 0..0;
            for frame in [
                0..0,
                0..3,
                1..4,
                2..6,
                4..7,
                6..10,
                8..10,
                10..10,
                11..13,
                11..14,
                13..14,
            ] {
                if frame.is_empty() {
                    sliding = SlidingInt64ByKeyAccumulator::new(is_max);
                } else {
                    sliding.update_batch(&slice(last.end, frame.end))?;
                    sliding.retract_batch(&slice(last.start, frame.start))?;
                }
                let mut original =
                    MaxMinByAccumulator::new(&DataType::Int64, &DataType::Int64, is_max)?;
                original.update_batch(&slice(frame.start, frame.end))?;
                assert_eq!(sliding.evaluate()?, original.evaluate()?, "{frame:?}");
                assert_eq!(sliding.state()?, original.state()?, "{frame:?}");
                last = frame;
            }
        }
        Ok(())
    }
}
