// Modified from Sail v0.7.1 for the Delta reader experiment. See experiments/spark-sql/UPSTREAM.md in the host repository.
use std::sync::Arc;

use datafusion::arrow::datatypes::FieldRef;
use datafusion::error::Result;
use datafusion::functions_window::ntile::Ntile;
use datafusion::logical_expr::function::{PartitionEvaluatorArgs, WindowUDFFieldArgs};
use datafusion::logical_expr::{PartitionEvaluator, Signature, WindowUDF, WindowUDFImpl};
use datafusion::scalar::ScalarValue;
use datafusion_physical_expr::expressions::Literal;

pub fn spark_ntile_udwf() -> Arc<WindowUDF> {
    Arc::new(WindowUDF::from(SparkNtile::new()))
}

/// DataFusion 54.1 distributes remainder rows into the first buckets.
/// Keep Sail's argument checks, including its signed 64-bit upper bound.
#[derive(Debug, Default, PartialEq, Eq, Hash)]
pub struct SparkNtile {
    inner: Ntile,
}

impl SparkNtile {
    pub fn new() -> Self {
        Self::default()
    }
}

impl WindowUDFImpl for SparkNtile {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn partition_evaluator(
        &self,
        args: PartitionEvaluatorArgs,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        let scalar_n = args
            .input_exprs()
            .first()
            .and_then(|expr| expr.downcast_ref::<Literal>())
            .map(Literal::value)
            .ok_or_else(|| {
                datafusion::common::exec_datafusion_err!("NTILE requires a positive integer")
            })?;
        if scalar_n.is_null() {
            return datafusion::common::exec_err!(
                "NTILE requires a positive integer, but found NULL"
            );
        }
        if get_integer_value(scalar_n)? <= 0 {
            return datafusion::common::exec_err!("NTILE requires a positive integer");
        }
        self.inner.partition_evaluator(args)
    }

    fn field(&self, args: WindowUDFFieldArgs) -> Result<FieldRef> {
        self.inner.field(args)
    }
}

/// Helper to extract integer value from ScalarValue
fn get_integer_value(scalar: &ScalarValue) -> Result<i64> {
    match scalar {
        ScalarValue::Int8(Some(v)) => Ok(*v as i64),
        ScalarValue::Int16(Some(v)) => Ok(*v as i64),
        ScalarValue::Int32(Some(v)) => Ok(*v as i64),
        ScalarValue::Int64(Some(v)) => Ok(*v),
        ScalarValue::UInt8(Some(v)) => Ok(*v as i64),
        ScalarValue::UInt16(Some(v)) => Ok(*v as i64),
        ScalarValue::UInt32(Some(v)) => Ok(*v as i64),
        ScalarValue::UInt64(Some(v)) => {
            if *v > i64::MAX as u64 {
                datafusion::common::exec_err!("NTILE argument too large")
            } else {
                Ok(*v as i64)
            }
        }
        ScalarValue::Null => datafusion::common::exec_err!("NTILE requires a non-null integer"),
        _ => datafusion::common::exec_err!("NTILE requires an integer argument"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::UInt64Array;
    use datafusion_physical_expr::PhysicalExpr;

    #[test]
    fn ntile_preserves_integer_validation() -> Result<()> {
        let function = SparkNtile::new();
        for value in [
            ScalarValue::Int8(Some(4)),
            ScalarValue::Int16(Some(4)),
            ScalarValue::Int32(Some(4)),
            ScalarValue::Int64(Some(4)),
            ScalarValue::UInt8(Some(4)),
            ScalarValue::UInt16(Some(4)),
            ScalarValue::UInt32(Some(4)),
            ScalarValue::UInt64(Some(4)),
        ] {
            let input: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::new(Literal::new(value))];
            let mut evaluator = function.partition_evaluator(PartitionEvaluatorArgs::new(
                &input,
                &[],
                false,
                false,
            ))?;
            let array = evaluator.evaluate_all(&[], 10)?;
            assert_eq!(
                array
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .values()
                    .as_ref(),
                &[1, 1, 1, 2, 2, 2, 3, 3, 4, 4]
            );
        }
        let input: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::new(Literal::new(ScalarValue::UInt64(
            Some(i64::MAX as u64),
        )))];
        let mut evaluator =
            function.partition_evaluator(PartitionEvaluatorArgs::new(&input, &[], false, false))?;
        let array = evaluator.evaluate_all(&[], 3)?;
        assert_eq!(
            array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .values()
                .as_ref(),
            &[1, 2, 3]
        );
        for (value, message) in [
            (
                ScalarValue::UInt64(Some(i64::MAX as u64 + 1)),
                "NTILE argument too large",
            ),
            (
                ScalarValue::Int64(Some(-1)),
                "NTILE requires a positive integer",
            ),
            (
                ScalarValue::Int32(Some(0)),
                "NTILE requires a positive integer",
            ),
            (
                ScalarValue::Int32(None),
                "NTILE requires a positive integer, but found NULL",
            ),
            (
                ScalarValue::Float64(Some(1.5)),
                "NTILE requires an integer argument",
            ),
        ] {
            let input: Vec<Arc<dyn PhysicalExpr>> = vec![Arc::new(Literal::new(value))];
            let error = function
                .partition_evaluator(PartitionEvaluatorArgs::new(&input, &[], false, false))
                .unwrap_err();
            assert_eq!(error.to_string(), format!("Execution error: {message}"));
        }
        assert_eq!(
            function
                .partition_evaluator(PartitionEvaluatorArgs::default())
                .unwrap_err()
                .to_string(),
            "Execution error: NTILE requires a positive integer"
        );
        Ok(())
    }
}
