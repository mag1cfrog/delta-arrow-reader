// Added for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::mem::size_of_val;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, AsArray, Int64Array};
use datafusion::arrow::compute::sum;
use datafusion::arrow::datatypes::{DataType, FieldRef, Int64Type};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::{Accumulator, AggregateUDFImpl, Signature};
use datafusion_spark::function::aggregate::try_sum::SparkTrySum;

#[derive(Debug, Default, PartialEq, Eq, Hash)]
pub struct TrySumWithBatchReduction(SparkTrySum);

impl AggregateUDFImpl for TrySumWithBatchReduction {
    fn name(&self) -> &str {
        self.0.name()
    }

    fn signature(&self) -> &Signature {
        self.0.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.0.return_type(arg_types)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        self.0.coerce_types(arg_types)
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        self.0.state_fields(args)
    }

    fn default_value(&self, data_type: &DataType) -> Result<ScalarValue> {
        self.0.default_value(data_type)
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let int64 = args.return_field.data_type() == &DataType::Int64;
        let checked_sum = self.0.accumulator(args)?;
        if int64 {
            Ok(Box::new(BatchReducingInt64Accumulator { checked_sum }))
        } else {
            Ok(checked_sum)
        }
    }
}

#[derive(Debug)]
struct BatchReducingInt64Accumulator {
    checked_sum: Box<dyn Accumulator>,
}

impl Accumulator for BatchReducingInt64Accumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if let [array] = values
            && let Some(input) = array.as_primitive_opt::<Int64Type>()
            && (2..=i32::MAX as usize).contains(&input.len())
            && let ScalarValue::Int64(Some(running_sum)) = self.checked_sum.evaluate()?
        {
            // Headroom for the whole batch also covers every ordered prefix.
            let bound = (input.len() as i64) << 31;
            if (i64::MIN + bound..=i64::MAX - bound).contains(&running_sum)
                && input
                    .values()
                    .iter()
                    .all(|&value| i32::try_from(value).is_ok())
            {
                if let Some(batch_sum) = sum(input) {
                    self.checked_sum
                        .update_batch(&[Arc::new(Int64Array::from_value(batch_sum, 1))])?;
                }
                return Ok(());
            }
        }
        self.checked_sum.update_batch(values)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.checked_sum.merge_batch(states)
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.checked_sum.state()
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        self.checked_sum.evaluate()
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.checked_sum.size()
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{Field, Schema};

    use super::*;

    #[test]
    fn batch_reduction_preserves_running_sum_and_terminal_overflow() -> Result<()> {
        let args = AccumulatorArgs {
            return_field: Arc::new(Field::new("sum", DataType::Int64, true)),
            schema: &Schema::empty(),
            expr_fields: &[],
            ignore_nulls: false,
            order_bys: &[],
            is_distinct: false,
            name: "try_sum",
            is_reversed: false,
            exprs: &[],
        };
        for (extreme, step) in [(i64::MAX, 1), (i64::MIN, -1)] {
            let mut original = SparkTrySum::new().accumulator(args.clone())?;
            let mut candidate = TrySumWithBatchReduction::default().accumulator(args.clone())?;
            for values in [
                vec![],
                vec![None, None],
                vec![None, Some(-10), Some(10)],
                vec![Some(i32::MAX as i64), Some(i32::MIN as i64), Some(1), None],
                vec![Some(extreme)],
                vec![Some(step), Some(-step)],
                vec![Some(-step), Some(-step)],
            ] {
                let input: ArrayRef = Arc::new(Int64Array::from(values));
                original.update_batch(std::slice::from_ref(&input))?;
                candidate.update_batch(&[input])?;
                assert_eq!(candidate.state()?, original.state()?);
            }
            assert_eq!(candidate.evaluate()?, ScalarValue::Int64(None));
        }
        Ok(())
    }
}
