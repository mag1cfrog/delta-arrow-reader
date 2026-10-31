use std::sync::{Arc, LazyLock};

use arrow::array::Int32Array;
use arrow::compute::{is_null, kernels::zip::zip};
use datafusion::arrow::datatypes::DataType;
use datafusion_common::{ScalarValue, utils::take_function_args};
use datafusion_expr::{
    ColumnarValue, ExprSchemable, ScalarUDF, Volatility, cast, create_udf, expr, lit, when,
};
use datafusion_functions::math::expr_fn::abs;
use datafusion_functions_nested::expr_fn;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::collection::spark_concat::SparkConcat;
use sail_function::scalar::collection::spark_reverse::SparkReverse;
use sail_function::scalar::misc::raise_error::RaiseError;

use crate::error::{PlanError, PlanResult};
use crate::function::common::{ScalarFunction, ScalarFunctionInput};

// COALESCE lowers to CASE and evaluates a non-null volatile input twice.
static NULL_SIZE_AS_MINUS_ONE: LazyLock<ScalarUDF> = LazyLock::new(|| {
    create_udf(
        "null_size_as_minus_one",
        vec![DataType::Int32],
        DataType::Int32,
        Volatility::Immutable,
        Arc::new(|arguments| {
            let [size] = take_function_args("null_size_as_minus_one", arguments)?;
            match size {
                ColumnarValue::Scalar(ScalarValue::Int32(None)) => {
                    Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(-1))))
                }
                ColumnarValue::Array(array) if array.null_count() > 0 => Ok(ColumnarValue::Array(
                    zip(&is_null(array)?, &Int32Array::new_scalar(-1), array)?,
                )),
                size => Ok(size.clone()),
            }
        }),
    )
});

fn size(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let value = input.arguments.one()?;

    let length = match value.get_type(input.function_context.schema)? {
        DataType::List(_)
        | DataType::ListView(_)
        | DataType::FixedSizeList(..)
        | DataType::LargeList(_)
        | DataType::LargeListView(_) => cast(expr_fn::array_length(value), DataType::Int32),
        DataType::Map(..) => cast(expr_fn::cardinality(value), DataType::Int32),
        DataType::Null => cast(value, DataType::Int32),
        wrong_type => {
            return Err(PlanError::InvalidArgument(format!(
                "size expects List or Map as argument, got {wrong_type:?}"
            )));
        }
    };
    let options = input.function_context.plan_config;
    if !options.ansi_mode && options.legacy_size_of_null {
        Ok(NULL_SIZE_AS_MINUS_ONE.call(vec![length]))
    } else {
        Ok(length)
    }
}

fn element_at(input: ScalarFunctionInput, is_try: bool) -> PlanResult<expr::Expr> {
    let (collection, element) = input.arguments.two()?;
    let (name, null_or_out_of_bounds) = if is_try {
        ("try_element_at", lit(ScalarValue::Null))
    } else {
        (
            "element_at",
            // TODO: respect spark.sql.ansi.enabled=false: https://spark.apache.org/docs/latest/api/sql/index.html#element_at
            ScalarUDF::from(RaiseError::new())
                .call(vec![lit("element_at: the index is out of bounds")]),
        )
    };

    Ok(match collection.get_type(input.function_context.schema)? {
        DataType::List(_)
        | DataType::ListView(_)
        | DataType::FixedSizeList(..)
        | DataType::LargeList(_)
        | DataType::LargeListView(_) => {
            when(element.clone().eq(lit(0)), ScalarUDF::from(RaiseError::new()).call(
            vec![lit(format!("{name}: the index 0 is invalid. An index shall be either < 0 or > 0 (the first element has index 1)"))]
                )).when( abs(element.clone()).not_between(lit(1), expr_fn::array_length(collection.clone())),
                null_or_out_of_bounds
                )
                .when(lit(true), expr_fn::array_element(collection, element)).end()?
        }
        DataType::Map(..) => {
            expr_fn::array_element(expr_fn::map_extract(collection, element), lit(1))
        }
        wrong_type => ScalarUDF::from(RaiseError::new()).call(vec![lit(format!(
            "{name} expects List or Map type as first argument, got {wrong_type:?}",
        ))]),
    })
}

pub(super) fn list_built_in_collection_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("cardinality", F::custom(size)),
        ("deep_size", F::unary(expr_fn::cardinality)),
        ("element_at", F::custom(|input| element_at(input, false))),
        ("size", F::custom(size)),
        ("array_concat", F::udf(SparkConcat::new())),
        ("concat", F::udf(SparkConcat::new())),
        ("reverse", F::udf(SparkReverse::new())),
        ("try_element_at", F::custom(|input| element_at(input, true))),
    ]
}
