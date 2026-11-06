// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion_common::ScalarValue;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_expr::{ExprSchemable, HigherOrderUDF, ScalarUDF, cast, expr, lit, when};
use datafusion_functions::math::expr_fn::abs;
use datafusion_functions_nested::expr_fn;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::collection::spark_collection_length::SparkCollectionLength;
use sail_function::scalar::collection::spark_concat::SparkConcat;
use sail_function::scalar::collection::spark_concat_runtime::SparkConcatRuntime;
use sail_function::scalar::collection::spark_reverse::SparkReverse;
use sail_function::scalar::misc::raise_error::RaiseError;

use crate::error::{PlanError, PlanResult};
use crate::function::common::{
    ScalarFunction, ScalarFunctionInput, drop_inherited_column_metadata,
    without_metadata_annotations,
};

use super::lambda::lambda_with_fresh_parameter;

// Lower only after logical optimization, so CombineConcats still sees its raw
// children. Simple columns/literals already use one safe flat kernel invocation.
pub(crate) fn lower_runtime_concat(expression: expr::Expr) -> PlanResult<Transformed<expr::Expr>> {
    let expr::Expr::ScalarFunction(function) = &expression else {
        return Ok(Transformed::no(expression));
    };
    let Some(concat) = function.func.inner().downcast_ref::<SparkConcat>() else {
        return Ok(Transformed::no(expression));
    };
    if !concat.has_ordered_evaluation()
        || function.args.iter().all(|arg| {
            matches!(
                without_metadata_annotations(arg),
                expr::Expr::Column(_) | expr::Expr::Literal(_, _)
            )
        })
    {
        return Ok(Transformed::no(expression));
    }
    // Native lambdas cannot contain async UDFs. Keep their existing physical
    // extraction path instead of introducing an unsupported lambda body.
    if expression.exists(|value| {
        Ok(matches!(value, expr::Expr::ScalarFunction(function)
            if function.func.as_async().is_some()))
    })? {
        return Ok(Transformed::no(expression));
    }
    let volatile = function.args.iter().any(expr::Expr::is_volatile);
    let runtime = SparkConcatRuntime::new(concat.clone(), volatile);
    let arguments = function
        .args
        .iter()
        .cloned()
        .map(|arg| lambda_with_fresh_parameter(arg, "_concat"))
        .collect::<PlanResult<Vec<_>>>()?;
    Ok(Transformed::yes(expr::Expr::HigherOrderFunction(
        expr::HigherOrderFunction::new(Arc::new(HigherOrderUDF::from(runtime)), arguments),
    )))
}

fn size(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let value = input.arguments.one()?;

    let length = match value.get_type(input.function_context.schema)? {
        DataType::List(_)
        | DataType::ListView(_)
        | DataType::FixedSizeList(..)
        | DataType::LargeList(_)
        | DataType::LargeListView(_) => cast(
            ScalarUDF::from(SparkCollectionLength::Array(Default::default())).call(vec![value]),
            DataType::Int32,
        ),
        DataType::Map(..) => cast(
            ScalarUDF::from(SparkCollectionLength::Map(Default::default())).call(vec![value]),
            DataType::Int32,
        ),
        DataType::Null => cast(value, DataType::Int32),
        wrong_type => {
            return Err(PlanError::InvalidArgument(format!(
                "size expects List or Map as argument, got {wrong_type:?}"
            )));
        }
    };
    let options = input.function_context.plan_config;
    if !options.ansi_mode && options.legacy_size_of_null {
        Ok(datafusion_functions::core::expr_fn::coalesce(vec![
            length,
            lit(-1i32),
        ]))
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

    let ordinary_concat = F::udf(SparkConcat::new());
    let ordered_concat = F::udf(SparkConcat::new().with_ordered_evaluation());
    let concat = F::custom(move |input| {
        let c06 = !input.arguments.is_empty()
            && input.arguments.iter().all(|arg| {
                matches!(
                    arg.get_type(input.function_context.schema),
                    Ok(DataType::Null
                        | DataType::Utf8
                        | DataType::LargeUtf8
                        | DataType::Utf8View
                        | DataType::Binary
                        | DataType::LargeBinary
                        | DataType::BinaryView
                        | DataType::FixedSizeBinary(_))
                )
            });
        if c06 {
            ordered_concat(input)
        } else {
            ordinary_concat(input)
        }
    });
    vec![
        ("cardinality", F::custom(size)),
        ("deep_size", F::unary(expr_fn::cardinality)),
        ("element_at", F::custom(|input| element_at(input, false))),
        ("size", F::custom(size)),
        (
            "concat",
            F::custom(move |input| {
                // Guard C06 STRING/BINARY identities while leaving multi-argument
                // and raw nested CONCATs visible to CombineConcats.
                let string_binary_identity = match input.arguments.as_slice() {
                    [value]
                        if !matches!(value, expr::Expr::ScalarFunction(function)
                            if function.func.inner().is::<SparkConcat>()) =>
                    {
                        matches!(
                            value.get_type(input.function_context.schema),
                            Ok(DataType::Utf8
                                | DataType::LargeUtf8
                                | DataType::Utf8View
                                | DataType::Binary
                                | DataType::LargeBinary)
                        )
                    }
                    _ => false,
                };
                let value = concat(input)?;
                Ok(if string_binary_identity {
                    drop_inherited_column_metadata(value)
                } else {
                    value
                })
            }),
        ),
        ("reverse", F::udf(SparkReverse::new())),
        ("try_element_at", F::custom(|input| element_at(input, true))),
    ]
}
