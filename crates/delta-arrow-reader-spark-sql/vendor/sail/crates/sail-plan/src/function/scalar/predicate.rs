// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::sync::Arc;

use datafusion::arrow::array::{AsArray, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::functions::{expr_fn, string as native_string};
use datafusion::physical_expr_common::datum::apply_cmp;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::type_coercion::binary::like_coercion;
use datafusion_expr::{
    ColumnarValue, ExprSchemable, Operator, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, Volatility, expr, not,
};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::predicate::spark_like::SparkLike;

use crate::error::{PlanError, PlanResult};
use crate::function::common::{ScalarFunction, ScalarFunctionInput, spark_in_list};
use crate::function::scalar::string::lower_expr;

fn extract_escape_char(escape_expr: expr::Expr, display_name: &str) -> PlanResult<char> {
    if let expr::Expr::Literal(
        ScalarValue::Utf8(Some(ref value))
        | ScalarValue::Utf8View(Some(ref value))
        | ScalarValue::LargeUtf8(Some(ref value)),
        _,
    ) = escape_expr
    {
        let mut chars = value.chars();
        if let (Some(escape), None) = (chars.next(), chars.next())
            && escape.len_utf16() == 1
        {
            return Ok(escape);
        }
    }
    Err(PlanError::invalid(format!(
        "[INVALID_ESCAPE_CHAR] `EscapeChar` should be a string literal of length one, but got \"{display_name}\"."
    )))
}

fn build_like_expr(input: ScalarFunctionInput, case_insensitive: bool) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        mut arguments,
        function_context,
    } = input;
    let n = arguments.len();
    if let [
        value @ expr::Expr::Column(_),
        literal @ expr::Expr::Literal(ScalarValue::Utf8(Some(pattern)), _),
    ] = arguments.as_slice()
        && let Some(prefix) = pattern.strip_suffix('%')
        && (1..=32).contains(&prefix.len())
        && prefix.bytes().all(|byte| byte.is_ascii_alphanumeric())
        && matches!(value.get_type(function_context.schema), Ok(DataType::Utf8))
    {
        if !case_insensitive {
            return Ok(value.clone().like(literal.clone()));
        }
        let nullable = value.nullable(function_context.schema)?;
        let matches = ScalarUDF::from(ILikeAsciiPrefix).call(vec![value.clone(), literal.clone()]);
        return Ok(ScalarUDF::from(ILikeResult { nullable }).call(vec![matches]));
    }
    let nullable = if case_insensitive && (2..=3).contains(&n) {
        let schema = function_context.schema;
        let nullable = arguments[0].nullable(schema)? || arguments[1].nullable(schema)?;
        let source_type = arguments[0].get_type(schema)?;
        let pattern_type = arguments[1].get_type(schema)?;
        let data_type = like_coercion(&source_type, &pattern_type).ok_or_else(|| {
            PlanError::invalid(format!("There isn't a common type to coerce {source_type} and {pattern_type} in ILIKE expression"))
        })?;
        for argument in &mut arguments[..2] {
            *argument = lower_expr(argument.clone().cast_to(&data_type, schema)?);
        }
        Some(nullable)
    } else {
        None
    };
    let result = match n {
        2 => {
            let (value, pattern) = arguments.two()?;
            Ok(ScalarUDF::from(SparkLike::new('\\')).call(vec![value, pattern]))
        }
        3 => {
            let (value, pattern, escape) = arguments.three()?;
            let display_name = function_context
                .argument_display_names
                .get(2)
                .ok_or_else(|| PlanError::invalid("missing LIKE escape display name"))?;
            let escape_char = extract_escape_char(escape, display_name)?;
            Ok(ScalarUDF::from(SparkLike::new(escape_char)).call(vec![value, pattern]))
        }
        _ => {
            let function_name = if case_insensitive { "ilike" } else { "like" };
            Err(PlanError::invalid(format!(
                "[WRONG_NUM_ARGS.WITHOUT_SUGGESTION] The `{function_name}` requires [2, 3] parameters but the actual number is {n}."
            )))
        }
    }?;
    Ok(match nullable {
        Some(nullable) => ScalarUDF::from(ILikeResult { nullable }).call(vec![result]),
        None => result,
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct ILikeAsciiPrefix;

impl ScalarUDFImpl for ILikeAsciiPrefix {
    fn name(&self) -> &str {
        "spark_ilike_ascii_prefix"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: std::sync::LazyLock<Signature> = std::sync::LazyLock::new(|| {
            Signature::exact(vec![DataType::Utf8, DataType::Utf8], Volatility::Immutable)
        });
        &SIGNATURE
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [value, pattern] = datafusion_common::utils::take_function_args(
            self.name(),
            std::mem::take(&mut args.args),
        )?;
        let [field, _] = args.arg_fields.as_slice() else {
            return datafusion_common::internal_err!(
                "{} requires two Utf8 argument fields",
                self.name()
            );
        };
        let ascii = match &value {
            ColumnarValue::Array(array) => array
                .as_string_opt::<i32>()
                .is_some_and(StringArray::is_ascii),
            ColumnarValue::Scalar(ScalarValue::Utf8(value)) => {
                value.as_ref().is_none_or(|value| value.is_ascii())
            }
            _ => false,
        };
        if ascii {
            return apply_cmp(Operator::ILikeMatch, &value, &pattern);
        }
        // Unicode case folding differs from lowercasing both operands.
        args.return_field = Arc::clone(field);
        args.arg_fields.truncate(1);
        args.args.push(value);
        let lower = native_string::lower().invoke_with_args(args)?;
        let ColumnarValue::Scalar(ScalarValue::Utf8(Some(prefix))) = pattern else {
            return datafusion_common::internal_err!("{} requires a literal prefix", self.name());
        };
        let pattern = ColumnarValue::Scalar(ScalarValue::Utf8(Some(prefix.to_ascii_lowercase())));
        apply_cmp(Operator::LikeMatch, &lower, &pattern)
    }
}

// Native LOWER supplies the values; ILIKE retains the original operands' fields.
#[derive(Debug, PartialEq, Eq, Hash)]
struct ILikeResult {
    nullable: bool,
}

impl ScalarUDFImpl for ILikeResult {
    fn name(&self) -> &str {
        "spark_ilike_result"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: std::sync::LazyLock<Signature> =
            std::sync::LazyLock::new(|| Signature::any(1, Volatility::Immutable));
        &SIGNATURE
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn return_field_from_args(&self, _args: ReturnFieldArgs) -> Result<FieldRef> {
        Ok(Arc::new(Field::new(
            self.name(),
            DataType::Boolean,
            self.nullable,
        )))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [result] = datafusion_common::utils::take_function_args(self.name(), args.args)?;
        Ok(result)
    }
}

fn rlike(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let (value, pattern) = arguments.two()?;
    // Short ASCII words have the same unanchored match in regex and LIKE.
    if let expr::Expr::Literal(ScalarValue::Utf8(Some(word)), _) = &pattern
        && (1..=32).contains(&word.len())
        && word.bytes().all(|byte| byte.is_ascii_alphanumeric())
        && matches!(value.get_type(function_context.schema), Ok(DataType::Utf8))
    {
        return Ok(value.like(datafusion_expr::lit(format!("%{word}%"))));
    }
    Ok(expr::Expr::SimilarTo(expr::Like {
        negated: false,
        expr: Box::new(value),
        pattern: Box::new(pattern),
        case_insensitive: false,
        escape_char: None,
    }))
}

fn is_in_list(input: ScalarFunctionInput) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        arguments,
        function_context,
    } = input;
    let (value, list) = arguments.at_least_one()?;
    spark_in_list(
        value,
        list,
        false,
        function_context.schema,
        function_context.plan_config,
    )
}

#[test]
fn numeric_in_preserves_nan_zero_and_null_semantics() -> PlanResult<()> {
    use crate::config::PlanConfig;
    use crate::function::common::FunctionContextInput;
    use arrow::array::{ArrayRef, AsArray, Float64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::physical_expr::create_physical_expr;
    use datafusion::prelude::SessionContext;
    use datafusion_common::DFSchema;
    use datafusion_expr::{col, execution_props::ExecutionProps, lit};

    let values = Float64Array::from(vec![
        Some(-0.0),
        Some(0.0),
        Some(f64::from_bits(0xfff0_0000_0000_1234)),
        Some(f64::NAN),
        Some(1.25),
        None,
    ]);
    let other = Float64Array::from(vec![
        Some(1.0),
        Some(1.0),
        Some(f64::NAN),
        Some(-f64::NAN),
        None,
        Some(1.0),
    ]);
    let ctx = SessionContext::new();
    let config = Arc::new(PlanConfig::default());
    for data_type in [DataType::Float32, DataType::Float64] {
        let schema = Arc::new(Schema::new(vec![
            Field::new("v", data_type.clone(), true),
            Field::new("other", data_type.clone(), true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                arrow::compute::cast(&values, &data_type)?,
                arrow::compute::cast(&other, &data_type)?,
            ],
        )?;
        let schema = Arc::new(DFSchema::try_from(schema)?);
        for dynamic in [false, true] {
            let list = if dynamic {
                vec![lit(ScalarValue::Decimal128(Some(0), 10, 2)), col("other")]
            } else {
                vec![
                    lit(ScalarValue::Decimal128(Some(0), 10, 2)),
                    lit(f64::NAN),
                    lit(ScalarValue::Null),
                ]
            };
            for negated in [false, true] {
                let expression = if negated {
                    spark_in_list(
                        col("v"),
                        list.clone(),
                        true,
                        &schema,
                        &PlanConfig {
                            ansi_mode: false,
                            ..Default::default()
                        },
                    )?
                } else {
                    is_in_list(ScalarFunctionInput {
                        arguments: std::iter::once(col("v")).chain(list.clone()).collect(),
                        function_context: FunctionContextInput {
                            argument_display_names: &[],
                            plan_config: &config,
                            session_context: &ctx,
                            schema: &schema,
                        },
                    })?
                };
                let physical = create_physical_expr(&expression, &schema, &ExecutionProps::new())?;
                let result: ArrayRef = physical.evaluate(&batch)?.into_array(batch.num_rows())?;
                let expected = [Some(true), Some(true), Some(true), Some(true), None, None]
                    .map(|v| v.map(|v| v != negated));
                assert_eq!(result.as_boolean().iter().collect::<Vec<_>>(), expected);
            }
        }
        let list = vec![lit(ScalarValue::Decimal128(Some(0), 10, 2)), lit("0")];
        let original = col("v").in_list(list.clone(), false);
        assert_eq!(
            spark_in_list(
                col("v"),
                list,
                false,
                &schema,
                &PlanConfig {
                    ansi_mode: false,
                    ..Default::default()
                }
            )?,
            original
        );
    }
    Ok(())
}

pub(super) fn list_built_in_predicate_functions() -> Vec<(&'static str, ScalarFunction)> {
    use crate::function::common::ScalarFunctionBuilder as F;

    vec![
        ("!", F::unary(not)),
        ("!=", F::binary_op(Operator::NotEq)),
        ("<", F::binary_op(Operator::Lt)),
        ("<=", F::binary_op(Operator::LtEq)),
        ("<=>", F::binary_op(Operator::IsNotDistinctFrom)),
        ("=", F::binary_op(Operator::Eq)),
        ("==", F::binary_op(Operator::Eq)),
        (">", F::binary_op(Operator::Gt)),
        (">=", F::binary_op(Operator::GtEq)),
        ("and", F::binary_op(Operator::And)),
        ("ilike", F::custom(|input| build_like_expr(input, true))),
        // TODO:
        //  If we want to prevent `IN` as a function in SQL,
        //  we can remove that from the built-in functions,
        //  and instead resolve it to spec::Expr::InList in the proto converter.
        ("in", F::custom(is_in_list)), // Spark passes isin as in
        ("isnan", F::unary(expr_fn::isnan)),
        (
            "isnotnull",
            F::unary(|x| expr::Expr::IsNotNull(Box::new(x))),
        ),
        ("isnull", F::unary(|x| expr::Expr::IsNull(Box::new(x)))),
        ("like", F::custom(|input| build_like_expr(input, false))),
        ("not", F::unary(not)),
        ("or", F::binary_op(Operator::Or)),
        ("regexp", F::custom(rlike)),
        ("regexp_like", F::custom(rlike)),
        ("rlike", F::custom(rlike)),
    ]
}
