use std::sync::Arc;

use datafusion::functions::expr_fn;
use datafusion_common::ScalarValue;
use datafusion_expr::{ScalarUDF, expr, lit};
use datafusion_spark::function::hash::expr_fn as hash_fn;
use datafusion_spark::function::math::hex::SparkHex;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::hash::spark_murmur3_hash::SparkMurmur3Hash;
use sail_function::scalar::hash::xxhash64_spark_input_encoding::Xxhash64WithSparkInputEncoding;

use crate::error::PlanResult;
use crate::function::common::{ScalarFunction, ScalarFunctionInput};

fn sha2(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput { arguments, .. } = input;
    let (expr, bit_length) = arguments.two()?;
    let result = match bit_length {
        expr::Expr::Literal(ScalarValue::Int32(Some(0)), _metadata)
        | expr::Expr::Literal(ScalarValue::Int32(Some(256)), _metadata) => expr_fn::sha256(expr),
        expr::Expr::Literal(ScalarValue::Int32(Some(224)), _metadata) => expr_fn::sha224(expr),
        expr::Expr::Literal(ScalarValue::Int32(Some(384)), _metadata) => expr_fn::sha384(expr),
        expr::Expr::Literal(ScalarValue::Int32(Some(512)), _metadata) => expr_fn::sha512(expr),
        expr::Expr::Literal(ScalarValue::Int32(Some(_)), _metadata) => {
            return Ok(lit(ScalarValue::Utf8(None)));
        }
        _ => {
            return Err(crate::error::PlanError::invalid(
                "The second argument of sha2 must be a literal integer.",
            ));
        }
    };
    let hex = expr::Expr::ScalarFunction(expr::ScalarFunction {
        func: Arc::new(ScalarUDF::from(SparkHex::new())),
        args: vec![result],
    });
    Ok(expr_fn::lower(hex))
}

pub(super) fn list_built_in_hash_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("crc32", F::unary(hash_fn::crc32)),
        ("hash", F::udf(SparkMurmur3Hash::new())),
        ("md5", F::unary(expr_fn::md5)),
        ("sha", F::unary(hash_fn::sha1)),
        ("sha1", F::unary(hash_fn::sha1)),
        ("sha2", F::custom(sha2)),
        (
            "xxhash64",
            F::udf(Xxhash64WithSparkInputEncoding::default()),
        ),
    ]
}
