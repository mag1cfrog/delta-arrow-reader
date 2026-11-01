//! Implicit comparison casts retain early local-input errors.
//! Explicit datetime functions have different pruning rules but share these parsers.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, TimeUnit};
use datafusion_common::{Result, plan_err};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
};
use sail_function::scalar::datetime::spark_date::SparkDate;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;

#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct SparkStringTemporalComparisonCast {
    parser: ScalarUDF,
}

impl SparkStringTemporalComparisonCast {
    pub(crate) fn try_new(target: &DataType, ansi_mode: bool) -> Result<Self> {
        let parser = match target {
            DataType::Date32 => ScalarUDF::from(SparkDate::new(!ansi_mode)),
            DataType::Timestamp(TimeUnit::Microsecond, timezone) => {
                ScalarUDF::from(SparkTimestamp::try_new(timezone.clone(), ansi_mode, false)?)
            }
            _ => return plan_err!("unsupported STRING temporal comparison type {target}"),
        };
        Ok(Self { parser })
    }
}

impl ScalarUDFImpl for SparkStringTemporalComparisonCast {
    fn name(&self) -> &str {
        "spark_string_temporal_comparison_cast"
    }

    fn signature(&self) -> &Signature {
        self.parser.signature()
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        self.parser.inner().return_type(args)
    }

    fn simplify(&self, args: Vec<Expr>, info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        if let [Expr::Literal(value, _)] = args.as_slice()
            && value.data_type().is_string()
        {
            // Implicit literal cast failures precede empty-result pruning.
            let result_type = self.return_type(&[value.data_type()])?;
            let value = self.invoke_with_args(ScalarFunctionArgs {
                args: vec![ColumnarValue::Scalar(value.clone())],
                arg_fields: vec![Arc::new(Field::new(
                    "input",
                    value.data_type(),
                    value.is_null(),
                ))],
                number_rows: 1,
                return_field: Arc::new(Field::new(self.name(), result_type, true)),
                config_options: info.config_options().clone(),
            })?;
            if let ColumnarValue::Scalar(value) = value {
                return Ok(ExprSimplifyResult::Simplified(Expr::Literal(value, None)));
            }
        }
        Ok(ExprSimplifyResult::Original(args))
    }

    fn invoke_with_args(&self, input: ScalarFunctionArgs) -> Result<ColumnarValue> {
        self.parser.invoke_with_args(input)
    }
}
