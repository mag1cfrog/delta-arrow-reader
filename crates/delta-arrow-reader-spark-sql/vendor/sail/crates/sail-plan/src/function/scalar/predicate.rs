// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::functions::expr_fn;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::type_coercion::binary::like_coercion;
use datafusion_expr::{
    ColumnarValue, ExprSchemable, Operator, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, Volatility, expr, lit, not,
};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::predicate::rewrite_like_pattern::RewriteLikePatternFunc;

use crate::error::{PlanError, PlanResult};
use crate::function::common::{ScalarFunction, ScalarFunctionInput, spark_in_list};

fn extract_escape_char(escape_expr: expr::Expr) -> PlanResult<Option<char>> {
    match escape_expr {
        expr::Expr::Literal(ScalarValue::Utf8(Some(ref s)), _)
        | expr::Expr::Literal(ScalarValue::Utf8View(Some(ref s)), _)
        | expr::Expr::Literal(ScalarValue::LargeUtf8(Some(ref s)), _) => {
            let mut chars = s.chars();
            match (chars.next(), chars.next()) {
                (Some(c), None) => Ok(Some(c)),
                _ => Err(PlanError::invalid(
                    "escape character must be a single character",
                )),
            }
        }
        _ => Err(PlanError::invalid(
            "escape character must be a string literal",
        )),
    }
}

fn build_like_expr(input: ScalarFunctionInput, case_insensitive: bool) -> PlanResult<expr::Expr> {
    let ScalarFunctionInput {
        mut arguments,
        function_context,
    } = input;
    let n = arguments.len();
    let nullable = if case_insensitive && (2..=3).contains(&n) {
        let schema = function_context.schema;
        let nullable = arguments[0].nullable(schema)? || arguments[1].nullable(schema)?;
        let source_type = arguments[0].get_type(schema)?;
        let pattern_type = arguments[1].get_type(schema)?;
        let data_type = like_coercion(&source_type, &pattern_type).ok_or_else(|| {
            PlanError::invalid(format!("There isn't a common type to coerce {source_type} and {pattern_type} in ILIKE expression"))
        })?;
        for argument in &mut arguments[..2] {
            *argument = expr_fn::lower(argument.clone().cast_to(&data_type, schema)?);
        }
        Some(nullable)
    } else {
        None
    };
    let result = match n {
        2 => {
            let (value, pattern) = arguments.two()?;
            Ok(expr::Expr::Like(expr::Like {
                negated: false,
                expr: Box::new(value),
                pattern: Box::new(pattern),
                case_insensitive: false,
                escape_char: None,
            }))
        }
        3 => {
            let (value, pattern, escape) = arguments.three()?;
            let escape_char = extract_escape_char(escape)?;
            // Arrow's LIKE kernel only supports `\` as the escape character.
            // For any other escape, wrap the pattern in a UDF that rewrites
            // it so Arrow sees `\` as the effective escape; then build an
            // `Expr::Like` with `escape_char: Some('\\')` to be explicit
            // about the escape that the rewritten pattern actually uses.
            let (pattern, escape_char) = match escape_char {
                Some(c) if c != '\\' => {
                    let rewritten = expr::Expr::ScalarFunction(expr::ScalarFunction {
                        func: Arc::new(ScalarUDF::from(RewriteLikePatternFunc::new())),
                        args: vec![pattern, lit(c.to_string())],
                    });
                    (rewritten, Some('\\'))
                }
                _ => (pattern, escape_char),
            };
            Ok(expr::Expr::Like(expr::Like {
                negated: false,
                expr: Box::new(value),
                pattern: Box::new(pattern),
                case_insensitive: false,
                escape_char,
            }))
        }
        _ => Err(PlanError::invalid(format!(
            "like/ilike expects 2 or 3 arguments, got {n}"
        ))),
    }?;
    Ok(match nullable {
        Some(nullable) => ScalarUDF::from(ILikeResult { nullable }).call(vec![result]),
        None => result,
    })
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

fn rlike(expr: expr::Expr, pattern: expr::Expr) -> expr::Expr {
    expr::Expr::SimilarTo(expr::Like {
        negated: false,
        expr: Box::new(expr),
        pattern: Box::new(pattern),
        case_insensitive: false,
        escape_char: None,
    })
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
        function_context.plan_config.ansi_mode,
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
    use datafusion_expr::{col, execution_props::ExecutionProps};

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
                    spark_in_list(col("v"), list.clone(), true, &schema, false)?
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
            spark_in_list(col("v"), list, false, &schema, false)?,
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
        ("regexp", F::binary(rlike)),
        ("regexp_like", F::binary(rlike)),
        ("rlike", F::binary(rlike)),
    ]
}
