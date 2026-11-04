// Local C06 runtime argument selection. Logical SparkConcat remains visible to optimizers.
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, UInt64Array, new_empty_array, new_null_array};
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{Result, exec_err, internal_datafusion_err};
use datafusion_expr::{
    ColumnarValue, HigherOrderFunctionArgs, HigherOrderReturnFieldArgs, HigherOrderSignature,
    HigherOrderUDFImpl, LambdaArgument, LambdaParametersProgress, ReturnFieldArgs,
    ScalarFunctionArgs, ScalarUDFImpl, ValueOrLambda, Volatility,
};

use super::spark_concat::SparkConcat;
use crate::functions_nested_utils::{evaluate_lambda_rows, scatter_active_rows};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkConcatRuntime {
    signature: HigherOrderSignature,
    concat: SparkConcat,
}

impl SparkConcatRuntime {
    pub fn new(concat: SparkConcat, volatile: bool) -> Self {
        Self {
            signature: HigherOrderSignature::variadic_any(if volatile {
                Volatility::Volatile
            } else {
                Volatility::Immutable
            }),
            concat,
        }
    }

    fn concatenate(
        &self,
        args: &HigherOrderFunctionArgs,
        values: Vec<ArrayRef>,
        fields: &[FieldRef],
        rows: usize,
    ) -> Result<ArrayRef> {
        self.concat
            .invoke_with_args(ScalarFunctionArgs {
                args: values.into_iter().map(ColumnarValue::Array).collect(),
                arg_fields: fields.to_vec(),
                number_rows: rows,
                return_field: Arc::clone(&args.return_field),
                config_options: Arc::clone(&args.config_options),
            })?
            .into_array(rows)
    }

    // Preserve batched invocation for volatile bodies, including seeded UDFs
    // whose existing state is initialized per invocation. Never replay an error.
    fn batched(
        &self,
        args: &HigherOrderFunctionArgs,
        lambdas: &[&LambdaArgument],
        fields: &[FieldRef],
    ) -> Result<ArrayRef> {
        let mut active = (0..args.number_rows)
            .map(u64::try_from)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|_| internal_datafusion_err!("CONCAT row count overflow"))?;
        let mut evaluated = Vec::with_capacity(lambdas.len());
        for lambda in lambdas {
            let value = evaluate_lambda_rows(lambda, &active)?;
            if value.len() != active.len() {
                return exec_err!(
                    "CONCAT operand returned {} rows for {} inputs",
                    value.len(),
                    active.len()
                );
            }
            let nulls = value.logical_nulls();
            let rows = active.clone();
            active = active
                .into_iter()
                .enumerate()
                .filter_map(|(index, row)| {
                    nulls
                        .as_ref()
                        .is_none_or(|nulls| nulls.is_valid(index))
                        .then_some(row)
                })
                .collect();
            evaluated.push((value, rows));
            if active.is_empty() {
                return Ok(new_null_array(args.return_type(), args.number_rows));
            }
        }
        // Align each argument once, after the final mask is known. Repeatedly
        // filtering all preceding STRING arrays would copy growing prefixes.
        let values = evaluated
            .into_iter()
            .map(|(value, rows)| {
                if rows == active {
                    return Ok(value);
                }
                let mut cursor = 0;
                let indices = active
                    .iter()
                    .map(|row| {
                        while rows.get(cursor).is_some_and(|source| source < row) {
                            cursor += 1;
                        }
                        if rows.get(cursor) != Some(row) {
                            return Err(internal_datafusion_err!(
                                "CONCAT selected row is absent from operand"
                            ));
                        }
                        u64::try_from(cursor).map_err(|_| {
                            internal_datafusion_err!("CONCAT selection index overflow")
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(take(value.as_ref(), &UInt64Array::from(indices), None)?)
            })
            .collect::<Result<Vec<_>>>()?;
        let value = self.concatenate(args, values, fields, active.len())?;
        scatter_active_rows(value, &active, args.number_rows)
    }
}

fn lambda_fields(fields: &[ValueOrLambda<FieldRef, FieldRef>]) -> Result<Vec<FieldRef>> {
    fields
        .iter()
        .map(|arg| match arg {
            ValueOrLambda::Lambda(field) => Ok(Arc::clone(field)),
            ValueOrLambda::Value(_) => exec_err!("CONCAT runtime expects internal lambdas"),
        })
        .collect()
}

impl HigherOrderUDFImpl for SparkConcatRuntime {
    fn name(&self) -> &str {
        "spark_concat_runtime"
    }

    fn signature(&self) -> &HigherOrderSignature {
        &self.signature
    }

    fn lambda_parameters(
        &self,
        _step: usize,
        fields: &[ValueOrLambda<FieldRef, Option<FieldRef>>],
    ) -> Result<LambdaParametersProgress> {
        if fields.is_empty()
            || fields
                .iter()
                .any(|arg| matches!(arg, ValueOrLambda::Value(_)))
        {
            return exec_err!("CONCAT runtime expects nonempty internal lambdas");
        }
        let dummy = Arc::new(Field::new("", DataType::Null, true));
        Ok(LambdaParametersProgress::Complete(
            fields.iter().map(|_| vec![Arc::clone(&dummy)]).collect(),
        ))
    }

    fn return_field_from_args(&self, args: HigherOrderReturnFieldArgs) -> Result<FieldRef> {
        let fields = lambda_fields(args.arg_fields)?;
        self.concat.return_field_from_args(ReturnFieldArgs {
            arg_fields: &fields,
            scalar_arguments: args.scalar_arguments,
        })
    }

    fn short_circuits(&self) -> bool {
        true
    }

    fn invoke_with_args(&self, args: HigherOrderFunctionArgs) -> Result<ColumnarValue> {
        if args.number_rows == 0 {
            return Ok(ColumnarValue::Array(new_empty_array(args.return_type())));
        }
        let lambdas = args
            .args
            .iter()
            .map(|arg| match arg {
                ValueOrLambda::Lambda(lambda) => Ok(lambda),
                ValueOrLambda::Value(_) => exec_err!("CONCAT runtime expects internal lambdas"),
            })
            .collect::<Result<Vec<_>>>()?;
        let fields = lambda_fields(&args.arg_fields)?;
        let value = self.batched(&args, &lambdas, &fields)?;
        Ok(ColumnarValue::Array(value))
    }
}
