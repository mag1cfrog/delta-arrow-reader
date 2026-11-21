// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray, Int64Array};
use datafusion::arrow::compute::kernels::{aggregate::min, arity::binary};
use datafusion::arrow::datatypes::{DataType, FieldRef, Int64Type};
use datafusion_common::{Result, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
};
use datafusion_spark::function::math::modulus::{SparkPmod as DataFusionPmod, spark_pmod};

/// Spark `pmod(a, b)` (positive modulo) that honors `spark.sql.ansi.enabled`.
///
/// The ANSI flag is captured at planning time (via the constructor) and
/// serialized through the physical codec, so the value the client requested
/// reaches every worker — unlike reading DataFusion's session-level
/// `execution.enable_ansi_mode`, which only reflects the driver's context.
///
/// Under ANSI mode a zero divisor raises an error; otherwise it returns NULL.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkPmod {
    inner: DataFusionPmod,
    ansi_mode: bool,
}

impl Default for SparkPmod {
    fn default() -> Self {
        Self::new(false)
    }
}

impl SparkPmod {
    pub fn new(ansi_mode: bool) -> Self {
        Self {
            inner: DataFusionPmod::new(),
            ansi_mode,
        }
    }
}

impl ScalarUDFImpl for SparkPmod {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn signature(&self) -> &Signature {
        self.inner.signature()
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!(
            "`return_type` should not be called; `return_field_from_args` is used instead"
        )
    }

    /// Spark: `Pmod.nullable = true`, unconditional (not narrowed by ANSI mode).
    /// <https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/arithmetic.scala#L1061>
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let field = self.inner.return_field_from_args(args)?;
        Ok(Arc::new(field.as_ref().clone().with_nullable(true)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if let [ColumnarValue::Array(left), ColumnarValue::Array(right)] = args.args.as_slice()
            && let (Some(left), Some(right)) = (
                left.as_primitive_opt::<Int64Type>(),
                right.as_primitive_opt::<Int64Type>(),
            )
            && left.len() == right.len()
            && min(right).is_some_and(|minimum| minimum > 0)
        {
            let result: Int64Array = binary(left, right, |left, right| {
                // The kernel may visit NULL slots. Only visible divisors must be positive.
                let remainder = left % right.max(1);
                if remainder < 0 {
                    remainder + right
                } else {
                    remainder
                }
            })?;
            return Ok(ColumnarValue::Array(Arc::new(result)));
        }
        spark_pmod(&args.args, self.ansi_mode)
    }
}

#[test]
fn positive_bigint_pmod_matches_the_existing_kernel() -> Result<()> {
    use datafusion::arrow::datatypes::Field;
    use datafusion_common::ScalarValue;

    let array = |values| ColumnarValue::Array(Arc::new(Int64Array::from(values)));
    let left = array(vec![
        None,
        Some(i64::MIN),
        Some(i64::MAX),
        Some(-5),
        Some(1),
    ]);
    let cases = [
        [
            left.clone(),
            array(vec![Some(2), Some(i64::MAX), None, Some(3), Some(1)]),
        ],
        [
            left.clone(),
            array(vec![Some(0), Some(1), Some(0), None, Some(1)]),
        ],
        [
            left.clone(),
            array(vec![None, Some(-1), Some(1), Some(i64::MIN), Some(1)]),
        ],
        [left, ColumnarValue::Scalar(ScalarValue::Int64(Some(7)))],
        [array(vec![]), array(vec![])],
    ];
    for ansi_mode in [false, true] {
        for args in &cases {
            let ColumnarValue::Array(left) = &args[0] else {
                unreachable!();
            };
            let number_rows = left.len();
            let field = Arc::new(Field::new("value", DataType::Int64, true));
            let expected = spark_pmod(args, ansi_mode);
            let actual = SparkPmod::new(ansi_mode).invoke_with_args(ScalarFunctionArgs {
                args: args.to_vec(),
                arg_fields: vec![field.clone(), field.clone()],
                number_rows,
                return_field: field,
                config_options: Arc::new(Default::default()),
            });
            match (expected, actual) {
                (Ok(expected), Ok(actual)) => {
                    assert_eq!(
                        expected.into_array(number_rows)?.as_ref(),
                        actual.into_array(number_rows)?.as_ref()
                    );
                }
                (Err(expected), Err(actual)) => {
                    assert_eq!(actual.to_string(), expected.to_string())
                }
                difference => panic!("{difference:?}"),
            }
        }
    }
    Ok(())
}
