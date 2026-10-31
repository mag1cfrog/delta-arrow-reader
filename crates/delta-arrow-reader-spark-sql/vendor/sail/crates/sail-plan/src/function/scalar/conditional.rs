// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::sync::Arc;

use arrow::datatypes::{DataType, TimeUnit};
use datafusion::functions::expr_fn;
use datafusion::functions::math::iszero::IsZeroFunc;
use datafusion_common::ScalarValue;
use datafusion_expr::{ExprSchemable, ScalarUDF, ScalarUDFImpl, cast, expr, lit};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::datetime::spark_date::SparkDate;
use sail_function::scalar::datetime::spark_timestamp::SparkTimestamp;
use sail_function::scalar::spark_to_string::SparkToUtf8;

use crate::error::{PlanError, PlanResult};
use crate::function::common::{
    FunctionContextInput, ScalarFunction, ScalarFunctionInput, cast_local_timestamp_to_ltz,
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
    let branch_values = coerce_conditional_temporal_values(branch_values, &function_context)?;
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
        coerce_conditional_temporal_values(vec![then_expr, else_expr], &function_context)?.two()?;
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
    let arguments = coerce_conditional_temporal_values(arguments, &function_context)?;
    Ok(expr_fn::coalesce(arguments))
}

fn ifnull(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let (left, right) = input.arguments.two()?;
    let schema = input.function_context.schema;
    if is_temporal_type(&left.get_type(schema)?) || is_temporal_type(&right.get_type(schema)?) {
        coalesce(ScalarFunctionInput {
            arguments: vec![left, right],
            function_context: input.function_context,
        })
    } else {
        // COALESCE's numeric/string coercion would change existing non-ANSI results.
        Ok(expr_fn::nvl(left, right))
    }
}

fn coerce_conditional_temporal_values(
    arguments: Vec<expr::Expr>,
    function_context: &FunctionContextInput<'_>,
) -> PlanResult<Vec<expr::Expr>> {
    let data_types = arguments
        .iter()
        .map(|arg| arg.get_type(function_context.schema))
        .collect::<Result<Vec<_>, _>>()?;
    let has_string = data_types.iter().any(is_string_type);
    let temporal_type =
        common_temporal_type(&data_types, &function_context.plan_config.session_timezone);
    let arguments = if has_string {
        if let Some(temporal_type) = temporal_type {
            if function_context.plan_config.ansi_mode {
                arguments
                    .into_iter()
                    .zip(data_types.iter())
                    .map(|(arg, data_type)| coerce_to_temporal(arg, data_type, &temporal_type))
                    .collect::<PlanResult<Vec<_>>>()?
            } else {
                arguments
                    .into_iter()
                    .zip(data_types)
                    .map(|(arg, data_type)| {
                        if is_temporal_type(&data_type) {
                            ScalarUDF::from(SparkToUtf8::new()).call(vec![arg])
                        } else {
                            arg
                        }
                    })
                    .collect()
            }
        } else {
            arguments
        }
    } else if let Some(temporal_type) = temporal_type {
        arguments
            .into_iter()
            .zip(data_types.iter())
            .map(|(arg, data_type)| coerce_to_temporal(arg, data_type, &temporal_type))
            .collect::<PlanResult<Vec<_>>>()?
    } else {
        arguments
    };
    Ok(arguments)
}

fn coerce_to_temporal(
    arg: expr::Expr,
    data_type: &DataType,
    target_type: &DataType,
) -> PlanResult<expr::Expr> {
    if data_type == target_type {
        return Ok(arg);
    }
    if is_string_type(data_type) {
        match target_type {
            DataType::Date32 => Ok(ScalarUDF::from(SparkDate::new(false)).call(vec![arg])),
            // This is only reached when ANSI mode requires a temporal common type.
            DataType::Timestamp(_, timezone) => {
                Ok(
                    ScalarUDF::from(SparkTimestamp::try_new(timezone.clone(), true, false)?)
                        .call(vec![arg]),
                )
            }
            _ => Ok(cast(arg, target_type.clone())),
        }
    } else if matches!(
        data_type,
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, None)
    ) && let DataType::Timestamp(TimeUnit::Microsecond, Some(timezone)) = target_type
    {
        Ok(cast_local_timestamp_to_ltz(arg, timezone))
    } else if is_temporal_type(data_type) {
        Ok(cast(arg, target_type.clone()))
    } else {
        Ok(arg)
    }
}

fn common_temporal_type(data_types: &[DataType], session_timezone: &Arc<str>) -> Option<DataType> {
    if data_types
        .iter()
        .any(|data_type| matches!(data_type, DataType::Timestamp(_, Some(_))))
    {
        Some(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some(Arc::clone(session_timezone)),
        ))
    } else if data_types
        .iter()
        .any(|data_type| matches!(data_type, DataType::Timestamp(_, None)))
    {
        Some(DataType::Timestamp(TimeUnit::Microsecond, None))
    } else {
        data_types
            .iter()
            .any(is_date_type)
            .then_some(DataType::Date32)
    }
}

fn is_string_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

fn is_temporal_type(data_type: &DataType) -> bool {
    is_date_type(data_type) || matches!(data_type, DataType::Timestamp(_, _))
}

fn is_date_type(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Date32 | DataType::Date64)
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
