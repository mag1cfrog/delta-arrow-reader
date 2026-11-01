// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion::functions::expr_fn;
use datafusion::functions::math::iszero::IsZeroFunc;
use datafusion_common::ScalarValue;
use datafusion_expr::{ExprSchemable, ScalarUDF, ScalarUDFImpl, expr, lit};
use sail_common_datafusion::utils::items::ItemTaker;

use crate::error::{PlanError, PlanResult};
use crate::function::common::{ScalarFunction, ScalarFunctionInput};
use crate::function::numeric_coercion::common_numeric_type;
use crate::function::result_coercion::{
    coerce_result_values, common_result_type_with_strings, is_temporal_type,
};

mod nullif;
pub(crate) use nullif::SparkNullIf;

fn case(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let mut conditions = Vec::new();
    let mut branch_values = Vec::new();
    let mut iter = arguments.into_iter();
    while let Some(condition) = iter.next() {
        match iter.next() {
            Some(result) => {
                let condition_type = condition.get_type(function_context.schema)?;
                if condition_type != DataType::Boolean {
                    return Err(PlanError::InvalidArgument(format!(
                        "CASE WHEN condition must be BOOLEAN, got {condition_type:?}"
                    )));
                }
                conditions.push(condition);
                branch_values.push(result);
            }
            _ => {
                conditions.push(lit(true));
                branch_values.push(condition);
                break;
            }
        }
    }
    let branch_values = coerce_result_values(branch_values, &function_context)?;
    let when_then_expr = conditions
        .into_iter()
        .zip(branch_values)
        .map(|(condition, value)| (Box::new(condition), Box::new(value)))
        .collect();
    Ok(expr::Expr::Case(expr::Case {
        expr: None, // Expr::Case in from_ast_expression incorporates into when_then_expr
        when_then_expr,
        else_expr: None,
    }))
}

fn if_expr(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let (when_expr, then_expr, else_expr) = arguments.three()?;
    let (then_expr, else_expr) =
        coerce_result_values(vec![then_expr, else_expr], &function_context)?.two()?;
    Ok(expr::Expr::Case(expr::Case {
        expr: None,
        when_then_expr: vec![(Box::new(when_expr), Box::new(then_expr))],
        else_expr: Some(Box::new(else_expr)),
    }))
}

fn coalesce(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let arguments = coerce_result_values(arguments, &function_context)?;
    Ok(expr_fn::coalesce(arguments))
}

fn ifnull(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let (left, right) = input.arguments.two()?;
    let schema = input.function_context.schema;
    let data_types = [left.get_type(schema)?, right.get_type(schema)?];
    if data_types.iter().any(is_temporal_type)
        || common_numeric_type(&data_types, input.function_context.plan_config.ansi_mode).is_some()
        || common_result_type_with_strings(
            &data_types,
            input.function_context.plan_config.ansi_mode,
        )?
        .is_some()
    {
        coalesce(ScalarFunctionInput {
            arguments: vec![left, right],
            function_context: input.function_context,
        })
    } else {
        Ok(expr_fn::nvl(left, right))
    }
}

pub(super) fn list_built_in_conditional_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("coalesce", F::custom(coalesce)),
        ("if", F::custom(if_expr)),
        ("ifnull", F::custom(ifnull)),
        ("nanvl", F::binary(expr_fn::nanvl)),
        (
            "nullif",
            F::custom(|input| {
                let (left, right) = input.arguments.two()?;
                Ok(ScalarUDF::from(SparkNullIf::new(
                    input.function_context.plan_config.ansi_mode,
                    input.function_context.plan_config.session_timezone.clone(),
                ))
                .call(vec![left, right]))
            }),
        ),
        ("nullifzero", F::custom(nullifzero)),
        ("nvl", F::custom(ifnull)),
        ("nvl2", F::ternary(expr_fn::nvl2)),
        ("zeroifnull", F::custom(zeroifnull)),
        ("when", F::custom(case)),
        ("case", F::custom(case)),
    ]
}

/// Create a zero literal with the same type as the input expression
fn create_zero_literal(data_type: &DataType) -> ScalarValue {
    match data_type {
        DataType::Int8 => ScalarValue::Int8(Some(0)),
        DataType::Int16 => ScalarValue::Int16(Some(0)),
        DataType::Int32 => ScalarValue::Int32(Some(0)),
        DataType::Int64 => ScalarValue::Int64(Some(0)),
        DataType::UInt8 => ScalarValue::UInt8(Some(0)),
        DataType::UInt16 => ScalarValue::UInt16(Some(0)),
        DataType::UInt32 => ScalarValue::UInt32(Some(0)),
        DataType::UInt64 => ScalarValue::UInt64(Some(0)),
        DataType::Float32 => ScalarValue::Float32(Some(0.0)),
        DataType::Float64 => ScalarValue::Float64(Some(0.0)),
        DataType::Decimal128(precision, scale) => {
            ScalarValue::Decimal128(Some(0), *precision, *scale)
        }
        DataType::Decimal256(precision, scale) => {
            ScalarValue::Decimal256(Some(0.into()), *precision, *scale)
        }
        // For non-numeric types, default to Int32
        _ => ScalarValue::Int32(Some(0)),
    }
}

// Reuse native zero detection and change validity only, preserving the input type.
#[derive(Debug, Default, PartialEq, Eq, Hash)]
pub(crate) struct SparkNullIfZero(IsZeroFunc);

impl ScalarUDFImpl for SparkNullIfZero {
    fn name(&self) -> &str {
        "spark_nullifzero"
    }

    fn signature(&self) -> &datafusion_expr::Signature {
        self.0.signature()
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn invoke_with_args(
        &self,
        input: datafusion_expr::ScalarFunctionArgs,
    ) -> datafusion_common::Result<datafusion_expr::ColumnarValue> {
        use arrow::array::AsArray;
        use arrow::compute::kernels::nullif::nullif;
        use datafusion_expr::ColumnarValue;
        let [value] =
            datafusion_common::utils::take_function_args(self.name(), input.args.as_slice())?;
        let value = value.clone();
        let zero = self
            .0
            .invoke_with_args(datafusion_expr::ScalarFunctionArgs {
                return_field: Arc::new(
                    input
                        .return_field
                        .as_ref()
                        .clone()
                        .with_data_type(DataType::Boolean),
                ),
                ..input
            })?;
        match (value, zero) {
            (ColumnarValue::Array(value), ColumnarValue::Array(zero)) => Ok(ColumnarValue::Array(
                nullif(value.as_ref(), zero.as_boolean())?,
            )),
            (
                ColumnarValue::Scalar(value),
                ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))),
            ) => Ok(ColumnarValue::Scalar(ScalarValue::try_new_null(
                &value.data_type(),
            )?)),
            (ColumnarValue::Scalar(value), ColumnarValue::Scalar(ScalarValue::Boolean(_))) => {
                Ok(ColumnarValue::Scalar(value))
            }
            _ => datafusion_common::internal_err!("iszero returned an unexpected value shape"),
        }
    }
}

#[test]
fn nullifzero_preserves_type_and_value_buffers() -> datafusion_common::Result<()> {
    use arrow::array::{Array, ArrayRef, Float16Array, Float32Array, Float64Array};
    use arrow::datatypes::Field;
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
    let run = |value: ColumnarValue, rows| {
        let field = Arc::new(Field::new("x", value.data_type(), true));
        SparkNullIfZero::default().invoke_with_args(ScalarFunctionArgs {
            args: vec![value],
            arg_fields: vec![field.clone()],
            number_rows: rows,
            return_field: field,
            config_options: Arc::new(ConfigOptions::default()),
        })
    };
    let values = [
        Some(-0.0),
        Some(0.0),
        None,
        Some(-2.0),
        Some(2.0),
        Some(f64::INFINITY),
        Some(f64::NAN),
    ];
    let arrays: [ArrayRef; 3] = [
        Arc::new(Float16Array::from_iter(
            values.map(|v| v.map(half::f16::from_f64)),
        )),
        Arc::new(Float32Array::from_iter(values.map(|v| v.map(|v| v as f32)))),
        Arc::new(Float64Array::from_iter(values)),
    ];
    for input in arrays {
        for (offset, len) in [(0, 7), (1, 4), (0, 0)] {
            let input = input.slice(offset, len);
            let output = run(ColumnarValue::Array(input.clone()), len)?.into_array(len)?;
            assert_eq!(output.data_type(), input.data_type());
            assert_eq!(
                output.to_data().buffers()[0].as_ptr(),
                input.to_data().buffers()[0].as_ptr()
            );
            for i in 0..len {
                assert_eq!(output.is_null(i), offset + i < 3);
            }
        }
        for index in 0..input.len() {
            let value = ScalarValue::try_from_array(&input, index)?;
            let result = run(ColumnarValue::Scalar(value.clone()), 1)?;
            let ColumnarValue::Scalar(result) = result else {
                panic!("expected scalar")
            };
            assert_eq!(result.data_type(), value.data_type());
            assert_eq!(result.is_null(), index < 3);
            if index >= 3 {
                assert_eq!(format!("{result:?}"), format!("{value:?}"));
            }
        }
    }
    Ok(())
}

/// Implementation of nullifzero function with type-aware casting
fn nullifzero(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let arg = arguments.one()?;

    // Get the data type of the input argument
    let data_type = arg.to_field(function_context.schema)?.1.data_type().clone();

    if data_type.is_floating() {
        return Ok(ScalarUDF::from(SparkNullIfZero::default()).call(vec![arg]));
    }

    // Create a zero literal with the same type as the input
    let zero_literal = lit(create_zero_literal(&data_type));

    // Return nullif(arg, zero_literal)
    Ok(expr_fn::nullif(arg, zero_literal))
}

/// Implementation of zeroifnull function with type-aware casting
fn zeroifnull(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let arg = arguments.one()?;

    // Get the data type of the input argument
    let data_type = arg.to_field(function_context.schema)?.1.data_type().clone();

    // Create a zero literal with the same type as the input
    let zero_literal = lit(create_zero_literal(&data_type));

    // Return nvl(arg, zero_literal)
    Ok(expr_fn::nvl(arg, zero_literal))
}
