// Local Spark integer-day DATE arithmetic. See the host crate's UPSTREAM.md.
use std::sync::Arc;

use datafusion::arrow::compute::kernels::numeric::{add_wrapping, sub_wrapping};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::arrow::error::ArrowError;
use datafusion::physical_expr_common::datum::apply;
use datafusion_common::{DataFusionError, Result, ScalarValue, internal_err};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    ColumnarValue, Expr, Operator, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility, lit,
};

use crate::scalar::spark_cast_string_to_integer::{
    SparkCastStringToInteger, StringIntegerCastMode,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkDateDayArithmetic {
    op: Operator,
    signature: Signature,
}

impl SparkDateDayArithmetic {
    pub fn new(op: Operator) -> Self {
        assert!(matches!(op, Operator::Plus | Operator::Minus));
        Self {
            op,
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkDateDayArithmetic {
    fn name(&self) -> &str {
        match self.op {
            Operator::Plus => "spark_date_add",
            Operator::Minus => "spark_date_sub",
            _ => unreachable!("constructor validates the operator"),
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        match types {
            [DataType::Date32, days] if *days == DataType::Int32 || days.is_string() => {
                Ok(DataType::Date32)
            }
            _ => internal_err!("{} requires DATE and integer or string days", self.name()),
        }
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let types = args
            .arg_fields
            .iter()
            .map(|field| field.data_type().clone())
            .collect::<Vec<_>>();
        Ok(Arc::new(Field::new(
            self.name(),
            self.return_type(&types)?,
            args.arg_fields.iter().any(|field| field.is_nullable()) || types[1].is_string(),
        )))
    }

    // A NULL start must skip the ANSI string-day conversion for that row.
    fn short_circuits(&self) -> bool {
        true
    }

    fn short_circuits_on_null_first_arg(&self) -> bool {
        true
    }

    fn conditional_arguments<'a>(
        &self,
        args: &'a [Expr],
    ) -> Option<(Vec<&'a Expr>, Vec<&'a Expr>)> {
        args.split_first()
            .map(|(first, rest)| (vec![first], rest.iter().collect()))
    }

    fn simplify(&self, args: Vec<Expr>, _info: &SimplifyContext) -> Result<ExprSimplifyResult> {
        if args
            .iter()
            .any(|arg| matches!(arg, Expr::Literal(value, _) if value.is_null()))
        {
            return Ok(ExprSimplifyResult::Simplified(lit(ScalarValue::Date32(
                None,
            ))));
        }
        Ok(ExprSimplifyResult::Original(args))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [date, days] = args.args.as_slice() else {
            return internal_err!("{} requires two arguments", self.name());
        };
        let days = if days.data_type().is_string() {
            SparkCastStringToInteger::try_new(DataType::Int32, StringIntegerCastMode::Ansi)?
                .invoke_with_args(ScalarFunctionArgs {
                    args: vec![days.clone()],
                    arg_fields: vec![args.arg_fields[1].clone()],
                    number_rows: args.number_rows,
                    return_field: Arc::new(Field::new("days", DataType::Int32, true)),
                    config_options: args.config_options,
                })
                .map_err(|error| match error {
                    DataFusionError::ArrowError(ref arrow, _)
                        if matches!(arrow.as_ref(), ArrowError::CastError(_)) =>
                    {
                        DataFusionError::Execution(format!("[CAST_INVALID_INPUT] {error}"))
                    }
                    other => other,
                })?
        } else {
            days.clone()
        };
        let date = date.cast_to(&DataType::Int32, None)?;
        // Spark wraps stored day counts in both ANSI modes, without a calendar conversion.
        let kernel = match self.op {
            Operator::Plus => add_wrapping,
            Operator::Minus => sub_wrapping,
            _ => unreachable!("constructor validates the operator"),
        };
        apply(&date, &days, kernel)?.cast_to(&DataType::Date32, None)
    }
}
