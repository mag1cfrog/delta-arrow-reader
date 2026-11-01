// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::logical_expr::expr::NullTreatment;
use datafusion::prelude::SessionContext;
use datafusion_common::tree_node::TreeNode;
use datafusion_common::{DFSchemaRef, ScalarValue};
use datafusion_expr::expr::{AggregateFunction, AggregateFunctionParams, WindowFunctionParams};
use datafusion_expr::{
    AggregateUDF, BinaryExpr, ExprSchemable, Operator, ScalarUDF, ScalarUDFImpl, WindowFrame,
    WindowFunctionDefinition, WindowUDF, cast, expr, lit, try_cast,
};
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::datetime::convert_tz::ConvertTz;
use sail_function::scalar::spark_cast_float_to_decimal::SparkCastFloatToDecimal;
use sail_function::scalar::spark_cast_string_to_decimal::SparkCastStringToDecimal;
use sail_function::scalar::variant::spark_cast_to_variant::SparkCastToVariant;
use sail_function::sketch::{DEFAULT_HLL_LG_CONFIG_K, DEFAULT_THETA_LG_NOM_ENTRIES};

use crate::config::PlanConfig;
use crate::error::{IntoPlanResult, PlanError, PlanResult};
use crate::function::is_spark_compatible_arrow_fixed_offset;

pub(crate) fn cast_local_timestamp_to_ltz(argument: expr::Expr, timezone: &Arc<str>) -> expr::Expr {
    let timestamp_ntz = cast(argument, DataType::Timestamp(TimeUnit::Microsecond, None));
    let timestamp_ltz = DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::clone(timezone)));
    if is_spark_compatible_arrow_fixed_offset(timezone.as_ref()) {
        return cast(timestamp_ntz, timestamp_ltz);
    }
    // Spark's named-zone conversion resolves DST gaps and overlaps differently from Arrow.
    let instant = ScalarUDF::from(ConvertTz::new(false)).call(vec![
        lit(timezone.to_string()),
        lit("UTC"),
        timestamp_ntz,
    ]);
    cast(cast(instant, DataType::Int64), timestamp_ltz)
}

pub struct FunctionContextInput<'a> {
    /// The names of function arguments.
    /// Most functions do not need this information, so it is
    /// passed as `&[String]` rather than `Vec<String>` to avoid unnecessary clone.
    /// These are the display names from the [`crate::resolver::expression::NamedExpr`]s,
    /// not to be confused with named function arguments
    /// (e.g., not like encode(charset => "utf-8", expr => "abc")).
    pub argument_display_names: &'a [String],
    pub plan_config: &'a Arc<PlanConfig>,
    pub session_context: &'a SessionContext,
    pub schema: &'a DFSchemaRef,
}

pub struct ScalarFunctionInput<'a> {
    pub arguments: Vec<expr::Expr>,
    pub function_context: FunctionContextInput<'a>,
}

pub(crate) type ScalarFunction =
    Arc<dyn Fn(ScalarFunctionInput) -> PlanResult<expr::Expr> + Send + Sync>;

pub(crate) struct ScalarFunctionBuilder;

impl ScalarFunctionBuilder {
    pub fn nullary<F, R>(f: F) -> ScalarFunction
    where
        F: Fn() -> R + Send + Sync + 'static,
        R: IntoPlanResult<expr::Expr>,
    {
        Arc::new(
            move |ScalarFunctionInput {
                      arguments,
                      function_context: _,
                  }| {
                arguments.zero()?;
                f().into_plan_result()
            },
        )
    }

    pub fn unary<F, R>(f: F) -> ScalarFunction
    where
        F: Fn(expr::Expr) -> R + Send + Sync + 'static,
        R: IntoPlanResult<expr::Expr>,
    {
        Arc::new(
            move |ScalarFunctionInput {
                      arguments,
                      function_context: _,
                  }| f(arguments.one()?).into_plan_result(),
        )
    }

    pub fn binary<F, R>(f: F) -> ScalarFunction
    where
        F: Fn(expr::Expr, expr::Expr) -> R + Send + Sync + 'static,
        R: IntoPlanResult<expr::Expr>,
    {
        Arc::new(
            move |ScalarFunctionInput {
                      arguments,
                      function_context: _,
                  }| {
                let (left, right) = arguments.two()?;
                f(left, right).into_plan_result()
            },
        )
    }

    pub fn ternary<F, R>(f: F) -> ScalarFunction
    where
        F: Fn(expr::Expr, expr::Expr, expr::Expr) -> R + Send + Sync + 'static,
        R: IntoPlanResult<expr::Expr>,
    {
        Arc::new(
            move |ScalarFunctionInput {
                      arguments,
                      function_context: _,
                  }| {
                let (first, second, third) = arguments.three()?;
                f(first, second, third).into_plan_result()
            },
        )
    }

    pub fn quaternary<F, R>(f: F) -> ScalarFunction
    where
        F: Fn(expr::Expr, expr::Expr, expr::Expr, expr::Expr) -> R + Send + Sync + 'static,
        R: IntoPlanResult<expr::Expr>,
    {
        Arc::new(
            move |ScalarFunctionInput {
                      arguments,
                      function_context: _,
                  }| {
                let (first, second, third, fourth) = arguments.four()?;
                f(first, second, third, fourth).into_plan_result()
            },
        )
    }

    pub fn var_arg<F, R>(f: F) -> ScalarFunction
    where
        F: Fn(Vec<expr::Expr>) -> R + Send + Sync + 'static,
        R: IntoPlanResult<expr::Expr>,
    {
        Arc::new(
            move |ScalarFunctionInput {
                      arguments,
                      function_context: _,
                  }| f(arguments).into_plan_result(),
        )
    }

    pub fn binary_op(op: Operator) -> ScalarFunction {
        Arc::new(
            move |ScalarFunctionInput {
                      arguments,
                      function_context,
                  }| {
                let (left, right) = arguments.two()?;
                spark_binary_expr(
                    left,
                    op,
                    right,
                    function_context.schema,
                    function_context.plan_config.ansi_mode,
                )
            },
        )
    }

    pub fn cast(data_type: DataType) -> ScalarFunction {
        Arc::new(
            move |ScalarFunctionInput {
                      arguments,
                      function_context,
                  }| {
                let arg = arguments.one()?;
                if let DataType::Decimal128(precision, scale) = data_type {
                    match arg.get_type(function_context.schema)? {
                        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                            return Ok(ScalarUDF::new_from_impl(SparkCastStringToDecimal::new(
                                precision,
                                scale,
                                !function_context.plan_config.ansi_mode,
                            ))
                            .call(vec![arg]));
                        }
                        DataType::Float32 | DataType::Float64 => {
                            return Ok(if function_context.plan_config.ansi_mode {
                                ScalarUDF::new_from_impl(SparkCastFloatToDecimal::new(
                                    precision, scale,
                                ))
                                .call(vec![arg])
                            } else {
                                try_cast(arg, data_type.clone())
                            });
                        }
                        _ => {}
                    }
                }
                Ok(cast(arg, data_type.clone()))
            },
        )
    }

    pub fn udf<F>(f: F) -> ScalarFunction
    where
        F: ScalarUDFImpl + Send + Sync + 'static,
    {
        let func = ScalarUDF::from(f);
        Arc::new(
            move |ScalarFunctionInput {
                      arguments,
                      function_context: _,
                  }| { Ok(func.call(arguments)) },
        )
    }

    pub fn custom<F>(f: F) -> ScalarFunction
    where
        F: Fn(ScalarFunctionInput) -> PlanResult<expr::Expr> + Send + Sync + 'static,
    {
        Arc::new(f)
    }

    pub fn unknown(name: &str) -> ScalarFunction {
        let name = name.to_string();
        Arc::new(move |_| Err(PlanError::todo(format!("function: {name}"))))
    }
}

fn numeric_literal(value: &expr::Expr) -> Option<ScalarValue> {
    match value {
        expr::Expr::Literal(value, _) => Some(value.clone()),
        expr::Expr::Cast(expr::Cast { expr, field })
        | expr::Expr::TryCast(expr::TryCast { expr, field }) => expr
            .as_literal()
            .filter(|value| value.data_type().is_numeric() || value.is_null())
            .and_then(|value| value.cast_to(field.data_type()).ok()),
        _ => None,
    }
}

fn integer_needs_double(value: &expr::Expr, data_type: &DataType) -> bool {
    // Every TINYINT/SMALLINT value is exactly representable as FLOAT.
    if !matches!(data_type, DataType::Int32 | DataType::Int64) {
        return false;
    }
    // Compare against Spark's DOUBLE value, including BIGINT's f64 rounding.
    // ponytail: reuse direct numeric literal evaluation; leave other constants
    // and dynamic INT/BIGINT values on the widening path.
    let double = match numeric_literal(value) {
        Some(ScalarValue::Int32(value)) => value.map(f64::from),
        Some(ScalarValue::Int64(value)) => value.map(|v| v as f64),
        _ => return true,
    };
    double.is_some_and(|v| f64::from(v as f32) != v)
}

pub(crate) fn spark_in_list(
    value: expr::Expr,
    list: Vec<expr::Expr>,
    negated: bool,
    schema: &DFSchemaRef,
    ansi_mode: bool,
) -> PlanResult<expr::Expr> {
    let mut input = expr::InList::new(Box::new(value), list, negated);
    let (mut decimal, mut floating, mut double, mut single, mut integer) =
        (false, false, false, false, false);
    for value in std::iter::once(input.expr.as_ref()).chain(&input.list) {
        let data_type = value.get_type(schema)?;
        // Leave string promotion and non-numeric coercion to the existing path.
        if !data_type.is_numeric() && data_type != DataType::Null {
            return Ok(expr::Expr::InList(input));
        }
        decimal |= data_type.is_decimal();
        floating |= data_type.is_floating();
        double |= data_type == DataType::Float64;
        single |= data_type == DataType::Float32;
        integer |= data_type.is_signed_integer();
    }
    if floating {
        // A BOOLEAN predicate can keep FLOAT if every integer has the same
        // value at FLOAT and Spark's DOUBLE comparison precision.
        let mut coerce_to_double = decimal;
        if !decimal && ansi_mode && single && integer && !double {
            for value in std::iter::once(input.expr.as_ref()).chain(&input.list) {
                if integer_needs_double(value, &value.get_type(schema)?) {
                    coerce_to_double = true;
                    break;
                }
            }
        }
        // Only zero and NaN can change membership when the input is normalized.
        // ponytail: recognize literals and direct numeric literal casts only;
        // leave other constant expressions on the existing normalization path.
        let (mut zero, mut negative_zero) = (false, false);
        let known_without_nan = matches!(
            input.expr.get_type(schema)?,
            DataType::Float32 | DataType::Float64
        ) && input.list.iter().all(|value| {
            let scalar = numeric_literal(value);
            match scalar.and_then(|value| match value {
                value @ ScalarValue::Float64(_) => Some(value),
                value => value.cast_to(&DataType::Float64).ok(),
            }) {
                Some(ScalarValue::Float64(value)) => value.is_none_or(|value| {
                    zero |= value == 0.0;
                    negative_zero |= value == 0.0 && value.is_sign_negative();
                    !value.is_nan()
                }),
                _ => false,
            }
        });
        let skip_normalization = known_without_nan && !zero;
        // Adding positive zero preserves NaNs and unifies both zero signs.
        // Keep computed operands and fused widening on their existing path.
        let zero_only = known_without_nan
            && zero
            && input.expr.try_as_col().is_some()
            && !(input.expr.get_type(schema)? == DataType::Float32 && (coerce_to_double || double));
        // Keep DOUBLE when Decimal or integer precision requires it. Preserve other types
        // so normalization does not change the existing numeric coercion.
        let convert = |value: expr::Expr, skip_normalization: bool| -> PlanResult<expr::Expr> {
            let data_type = value.get_type(schema)?;
            let floating = data_type.is_floating();
            Ok(if floating && !skip_normalization {
                ScalarUDF::from(SparkComparisonFloat {
                    widen: coerce_to_double || double,
                })
                .call(vec![value])
            } else if data_type != DataType::Float64 && (coerce_to_double || (floating && double)) {
                cast(value, DataType::Float64)
            } else {
                value
            })
        };
        input.expr = Box::new(if zero_only {
            let zero = ScalarValue::new_zero(&input.expr.get_type(schema)?)?;
            *input.expr + lit(zero)
        } else {
            convert(*input.expr, skip_normalization)?
        });
        input.list = input
            .list
            .into_iter()
            .map(|value| convert(value, known_without_nan && !negative_zero))
            .collect::<PlanResult<_>>()?;
    }
    Ok(expr::Expr::InList(input))
}

pub(crate) fn spark_comparison_widens_to_double(
    left: &DataType,
    right: &DataType,
    widen_integer_float: bool,
) -> bool {
    (left.is_decimal() && right.is_floating())
        || (left.is_floating() && right.is_decimal())
        || (widen_integer_float
            && ((left == &DataType::Float32 && right.is_signed_integer())
                || (right == &DataType::Float32 && left.is_signed_integer())))
}

pub(crate) fn spark_binary_expr(
    mut left: expr::Expr,
    op: Operator,
    mut right: expr::Expr,
    schema: &DFSchemaRef,
    ansi_mode: bool,
) -> PlanResult<expr::Expr> {
    if matches!(
        op,
        Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
            | Operator::IsDistinctFrom
            | Operator::IsNotDistinctFrom
    ) {
        let left_type = left.get_type(schema)?;
        let right_type = right.get_type(schema)?;
        if (left_type.is_numeric() || left_type == DataType::Null)
            && (right_type.is_numeric() || right_type == DataType::Null)
        {
            let ansi_float_integer = ansi_mode
                && ((left_type == DataType::Float32 && integer_needs_double(&right, &right_type))
                    || (right_type == DataType::Float32
                        && integer_needs_double(&left, &left_type)));
            let promote_to_double =
                spark_comparison_widens_to_double(&left_type, &right_type, ansi_float_integer);
            let widen = promote_to_double
                || left_type == DataType::Float64
                || right_type == DataType::Float64;
            let convert = |value, data_type: DataType| {
                if data_type.is_floating() {
                    ScalarUDF::from(SparkComparisonFloat { widen }).call(vec![value])
                } else if promote_to_double {
                    cast(value, DataType::Float64)
                } else {
                    value
                }
            };
            left = convert(left, left_type);
            right = convert(right, right_type);
        }
    }
    Ok(expr::Expr::BinaryExpr(BinaryExpr::new(
        Box::new(left),
        op,
        Box::new(right),
    )))
}

#[test]
fn in_list_skips_only_irrelevant_float_normalization() -> datafusion_common::Result<()> {
    use arrow::array::{Array, ArrayRef, AsArray, Float32Array, Float64Array};
    use arrow::datatypes::{Field, Float64Type, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::optimizer::simplify_expressions::ExprSimplifier;
    use datafusion::physical_expr::{create_physical_expr, execution_props::ExecutionProps};
    use datafusion_common::{DFSchema, tree_node::TreeNodeRecursion};
    use datafusion_expr::{col, simplify::SimplifyContext};

    let half_schema = Arc::new(DFSchema::try_from(Schema::new(vec![Field::new(
        "v",
        DataType::Float16,
        true,
    )]))?);
    let half = spark_in_list(col("v"), vec![lit(1.25_f32)], false, &half_schema, false).unwrap();
    let expr::Expr::InList(half) = half else {
        unreachable!()
    };
    assert!(matches!(half.expr.as_ref(), expr::Expr::ScalarFunction(f)
        if f.func.inner().is::<SparkComparisonFloat>()));

    let cast_schema = Arc::new(DFSchema::try_from(Schema::new(vec![
        Field::new("decimal", DataType::Decimal128(18, 4), true),
        Field::new("integer", DataType::Int64, true),
    ]))?);
    for name in ["decimal", "integer"] {
        let expr::Expr::InList(input) = spark_in_list(
            cast(col(name), DataType::Float64),
            vec![lit(0.0_f64), lit(1.25_f64)],
            false,
            &cast_schema,
            false,
        )
        .unwrap() else {
            unreachable!()
        };
        assert_eq!(input.list.len(), 2, "preserve inverse-cast candidates");
        assert!(matches!(input.expr.as_ref(), expr::Expr::ScalarFunction(f)
            if f.func.inner().is::<SparkComparisonFloat>()));
    }

    let mut values = vec![
        None,
        Some(0.0),
        Some(-0.0),
        Some(1.25),
        Some(1.5),
        Some(-1.5),
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
        Some(f64::NAN),
        Some(f64::from_bits(0xfff0_0000_0000_0123)),
        Some(1e-40),
        Some(-1e-40),
    ];
    let mut bits = 0x1234_5678_9abc_def0_u64;
    values.extend((0..8192).map(|i| {
        bits = bits.wrapping_mul(6364136223846793005).wrapping_add(1);
        (i % 19 != 0).then_some(f64::from_bits(bits))
    }));
    let arrays: [ArrayRef; 2] = [
        Arc::new(Float32Array::from_iter(
            values.iter().map(|v| v.map(|v| v as f32)),
        )),
        Arc::new(Float64Array::from(values)),
    ];
    let mut checks = 0;
    let mut checked_rows = 0;
    for input in arrays {
        let ty = input.data_type().clone();
        let literal = |v: Option<f64>| {
            lit(match ty {
                DataType::Float32 => ScalarValue::Float32(v.map(|v| v as f32)),
                _ => ScalarValue::Float64(v),
            })
        };
        let finite = vec![Some(1.25), Some(1.5), Some(2.0)];
        let mut cases = vec![
            (
                "finite",
                finite.iter().copied().map(literal).collect::<Vec<_>>(),
                finite.clone(),
                true,
            ),
            (
                "nullable",
                vec![literal(Some(1.25)), literal(None)],
                vec![Some(1.25), None],
                true,
            ),
            (
                "zero",
                vec![literal(Some(0.0)), literal(Some(1.25))],
                vec![Some(0.0), Some(1.25)],
                true,
            ),
            (
                "negative_zero",
                vec![literal(Some(-0.0))],
                vec![Some(-0.0)],
                true,
            ),
            (
                "nan",
                vec![literal(Some(f64::NAN))],
                vec![Some(f64::NAN)],
                false,
            ),
            (
                "negative_nan",
                vec![literal(Some(-f64::NAN))],
                vec![Some(f64::NAN)],
                false,
            ),
            (
                "infinity",
                vec![
                    literal(Some(f64::INFINITY)),
                    literal(Some(f64::NEG_INFINITY)),
                ],
                vec![Some(f64::INFINITY), Some(f64::NEG_INFINITY)],
                true,
            ),
            ("all_null", vec![literal(None)], vec![None], true),
            ("empty", vec![], vec![], true),
            (
                "integer",
                vec![lit(1_i64), lit(2_i64)],
                vec![Some(1.0), Some(2.0)],
                true,
            ),
            (
                "decimal",
                vec![lit(ScalarValue::Decimal128(Some(125), 5, 2))],
                vec![Some(1.25)],
                true,
            ),
            (
                "casts",
                vec![cast(
                    lit(ScalarValue::Decimal128(Some(125), 5, 2)),
                    ty.clone(),
                )],
                vec![Some(1.25)],
                true,
            ),
            (
                "try_casts",
                vec![try_cast(lit(1_i64), ty.clone())],
                vec![Some(1.0)],
                true,
            ),
            (
                "underflow",
                vec![cast(lit(1e-50_f64), DataType::Float32)],
                vec![Some(0.0)],
                true,
            ),
            (
                "zero_nullable",
                vec![literal(Some(0.0)), literal(None)],
                vec![Some(0.0), None],
                true,
            ),
            (
                "zero_both_signs",
                vec![literal(Some(0.0)), literal(Some(-0.0))],
                vec![Some(0.0), Some(-0.0)],
                true,
            ),
            (
                "zero_integer",
                vec![lit(0_i64), lit(1_i64)],
                vec![Some(0.0), Some(1.0)],
                true,
            ),
            (
                "zero_decimal",
                vec![
                    lit(ScalarValue::Decimal128(Some(0), 5, 2)),
                    lit(ScalarValue::Decimal128(Some(125), 5, 2)),
                ],
                vec![Some(0.0), Some(1.25)],
                ty == DataType::Float64,
            ),
            (
                "zero_widening",
                vec![lit(0.0_f64), lit(1.25_f64)],
                vec![Some(0.0), Some(1.25)],
                ty == DataType::Float64,
            ),
            (
                "zero_negative_underflow",
                vec![cast(lit(-1e-50_f64), DataType::Float32)],
                vec![Some(-0.0)],
                true,
            ),
            (
                "zero_half",
                vec![cast(lit(0.0_f64), DataType::Float16)],
                vec![Some(0.0)],
                true,
            ),
            (
                "negative_zero_half",
                vec![cast(lit(-0.0_f64), DataType::Float16)],
                vec![Some(-0.0)],
                true,
            ),
            (
                "zero_nan",
                vec![literal(Some(0.0)), literal(Some(-f64::NAN))],
                vec![Some(0.0), Some(f64::NAN)],
                false,
            ),
            (
                "zero_unknown_cast",
                vec![literal(Some(0.0)), cast(lit("1.25"), ty.clone())],
                vec![Some(0.0), Some(1.25)],
                false,
            ),
            (
                "cast_nan",
                vec![cast(lit(-f64::NAN), DataType::Float32)],
                vec![Some(f64::NAN)],
                false,
            ),
            (
                "string_cast",
                vec![cast(lit("1.25"), ty.clone())],
                vec![Some(1.25)],
                false,
            ),
            (
                "nested_cast",
                vec![cast(cast(lit(1_i64), DataType::Float32), ty.clone())],
                vec![Some(1.0)],
                false,
            ),
            (
                "dynamic",
                vec![col("v"), literal(Some(1.25))],
                vec![Some(1.25)],
                false,
            ),
        ];
        let long: Vec<_> = (1..=128).map(|v| Some(f64::from(v))).collect();
        cases.push((
            "long",
            long.iter().copied().map(literal).collect(),
            long,
            true,
        ));
        let long_zero: Vec<_> = (0..=128).map(|v| Some(f64::from(v))).collect();
        cases.push((
            "zero_long",
            long_zero.iter().copied().map(literal).collect(),
            long_zero,
            true,
        ));
        for (offset, rows) in [(0, input.len()), (1, input.len() - 1), (2, 1), (0, 0)] {
            let input = input.slice(offset, rows);
            let schema = Arc::new(Schema::new(vec![Field::new("v", ty.clone(), true)]));
            let batch = RecordBatch::try_new(schema.clone(), vec![input.clone()])?;
            let schema = Arc::new(DFSchema::try_from(schema)?);
            let simplifier = ExprSimplifier::new(
                SimplifyContext::builder()
                    .with_schema(schema.clone())
                    .build(),
            );
            let wide = arrow::compute::cast(&input, &DataType::Float64)?;
            for (name, list, expected, skip) in &cases {
                for negated in [false, true] {
                    let candidate =
                        spark_in_list(col("v"), list.clone(), negated, &schema, false).unwrap();
                    let expr::Expr::InList(in_list) = &candidate else {
                        unreachable!()
                    };
                    let mut normalizers = 0;
                    in_list.expr.apply(|e| {
                        if let expr::Expr::ScalarFunction(f) = e {
                            normalizers += usize::from(f.func.inner().is::<SparkComparisonFloat>());
                        }
                        Ok(TreeNodeRecursion::Continue)
                    })?;
                    assert_eq!(normalizers, usize::from(!skip), "{ty} {name}");
                    assert_eq!(
                        in_list.list.len(),
                        list.len(),
                        "preserve native IN dispatch"
                    );
                    if matches!(*name, "zero" | "zero_nullable" | "zero_long") {
                        assert!(
                            in_list.list.iter().all(|value| {
                                !matches!(value, expr::Expr::ScalarFunction(f)
                                if f.func.inner().is::<SparkComparisonFloat>())
                            }),
                            "canonical constants do not need normalization: {ty} {name}"
                        );
                    }
                    if *skip && expected.contains(&Some(0.0)) {
                        assert!(
                            matches!(
                                in_list.expr.as_ref(),
                                expr::Expr::BinaryExpr(BinaryExpr {
                                    op: Operator::Plus,
                                    ..
                                })
                            ),
                            "{ty} {name}"
                        );
                    } else if *skip && ty == DataType::Float64 {
                        // An identity cast can hide native IN and inverse-Decimal rewrites.
                        assert_eq!(in_list.expr.as_ref(), &col("v"));
                    }
                    let normalized =
                        ScalarUDF::from(SparkComparisonFloat { widen: false }).call(vec![col("v")]);
                    let baseline =
                        spark_in_list(normalized, list.clone(), negated, &schema, false).unwrap();
                    assert_eq!(baseline.get_type(&schema)?, candidate.get_type(&schema)?);
                    assert_eq!(baseline.nullable(&schema)?, candidate.nullable(&schema)?);
                    for expression in [baseline, candidate] {
                        let expression =
                            simplifier.simplify(simplifier.coerce(expression, &schema)?)?;
                        let result =
                            create_physical_expr(&expression, &schema, &ExecutionProps::new())?
                                .evaluate(&batch)?
                                .into_array(rows)?;
                        for (i, value) in wide.as_primitive::<Float64Type>().iter().enumerate() {
                            let found = if expected.is_empty() && *name != "dynamic" {
                                Some(false)
                            } else {
                                value.and_then(|v| {
                                    if *name == "dynamic"
                                        || expected
                                            .iter()
                                            .flatten()
                                            .any(|w| v == *w || (v.is_nan() && w.is_nan()))
                                    {
                                        Some(true)
                                    } else if expected.contains(&None) {
                                        None
                                    } else {
                                        Some(false)
                                    }
                                })
                            }
                            .map(|v| v != negated);
                            assert_eq!(
                                result.as_boolean().is_null(i),
                                found.is_none(),
                                "{ty} {name} {i}"
                            );
                            if let Some(found) = found {
                                assert_eq!(result.as_boolean().value(i), found, "{ty} {name} {i}");
                            }
                            checked_rows += 1;
                        }
                        checks += 1;
                    }
                }
            }
        }
    }
    assert_eq!(checks, 960);
    assert_eq!(checked_rows, 3937920);
    Ok(())
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct SparkComparisonFloat {
    pub(crate) widen: bool,
}

impl ScalarUDFImpl for SparkComparisonFloat {
    fn name(&self) -> &str {
        "spark_comparison_float"
    }

    fn signature(&self) -> &datafusion_expr::Signature {
        static SIGNATURE: std::sync::LazyLock<datafusion_expr::Signature> =
            std::sync::LazyLock::new(|| {
                datafusion_expr::Signature::uniform(
                    1,
                    vec![DataType::Float32, DataType::Float64],
                    datafusion_expr::Volatility::Immutable,
                )
            });
        &SIGNATURE
    }

    fn return_type(&self, args: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(if self.widen {
            DataType::Float64
        } else {
            args[0].clone()
        })
    }

    fn return_field_from_args(
        &self,
        args: datafusion_expr::ReturnFieldArgs,
    ) -> datafusion_common::Result<arrow::datatypes::FieldRef> {
        Ok(Arc::new(arrow::datatypes::Field::new(
            self.name(),
            self.return_type(&[args.arg_fields[0].data_type().clone()])?,
            args.arg_fields[0].is_nullable(),
        )))
    }

    fn simplify(
        &self,
        args: Vec<expr::Expr>,
        info: &datafusion_expr::simplify::SimplifyContext,
    ) -> datafusion_common::Result<datafusion_expr::simplify::ExprSimplifyResult> {
        use datafusion_expr::simplify::ExprSimplifyResult;
        let canonical = match &args[0] {
            expr::Expr::ScalarFunction(f) => f.func.inner().is::<Self>(),
            expr::Expr::Cast(expr::Cast { expr, field })
            | expr::Expr::TryCast(expr::TryCast { expr, field }) => {
                let source = info.get_data_type(expr)?;
                // Integer casts cannot produce -0 or NaN. Decimal-to-DOUBLE
                // cannot underflow; Decimal-to-FLOAT can, so keep its check.
                source.is_integer()
                    || (source.is_decimal() && field.data_type() == &DataType::Float64)
            }
            _ => false,
        };
        if !canonical {
            return Ok(ExprSimplifyResult::Original(args));
        }
        let [arg] = datafusion_common::utils::take_function_args(self.name(), args)?;
        Ok(ExprSimplifyResult::Simplified(
            if self.widen && info.get_data_type(&arg)? == DataType::Float32 {
                cast(arg, DataType::Float64)
            } else {
                arg
            },
        ))
    }

    fn invoke_with_args(
        &self,
        input: datafusion_expr::ScalarFunctionArgs,
    ) -> datafusion_common::Result<datafusion_expr::ColumnarValue> {
        use arrow::array::AsArray;
        use arrow::datatypes::{Float32Type, Float64Type};
        use datafusion_expr::ColumnarValue;
        // Fuse FLOAT widening with normalization; evaluate the operand once.
        // Adding positive zero clears the sign of -0; NaNs still need explicit canonicalization.
        let normalize = |v: f64| {
            if v.is_nan() { f64::NAN } else { v + 0.0 }
        };
        let [value] = datafusion_common::utils::take_function_args(self.name(), input.args)?;
        if !self.widen && value.data_type() == DataType::Float32 {
            let normalize = |v: f32| {
                if v.is_nan() { f32::NAN } else { v + 0.0 }
            };
            return Ok(match value {
                ColumnarValue::Scalar(ScalarValue::Float32(v)) => {
                    ColumnarValue::Scalar(ScalarValue::Float32(v.map(normalize)))
                }
                ColumnarValue::Array(a) => ColumnarValue::Array(Arc::new(
                    a.as_primitive::<Float32Type>()
                        .unary::<_, Float32Type>(normalize),
                )),
                _ => unreachable!("FLOAT operand checked above"),
            });
        }
        Ok(match value {
            ColumnarValue::Scalar(ScalarValue::Float32(v)) => {
                ColumnarValue::Scalar(ScalarValue::Float64(v.map(|v| normalize(f64::from(v)))))
            }
            ColumnarValue::Scalar(ScalarValue::Float64(v)) => {
                ColumnarValue::Scalar(ScalarValue::Float64(v.map(normalize)))
            }
            ColumnarValue::Array(a) if a.data_type() == &DataType::Float32 => {
                ColumnarValue::Array(Arc::new(
                    a.as_primitive::<Float32Type>()
                        .unary::<_, Float64Type>(|v| normalize(f64::from(v))),
                ))
            }
            ColumnarValue::Array(a) if a.data_type() == &DataType::Float64 => {
                ColumnarValue::Array(Arc::new(
                    a.as_primitive::<Float64Type>()
                        .unary::<_, Float64Type>(normalize),
                ))
            }
            _ => {
                return datafusion_common::internal_err!(
                    "expected FLOAT or DOUBLE comparison operand"
                );
            }
        })
    }
}

#[test]
fn comparison_float_preserves_values_nulls_and_shapes() -> datafusion_common::Result<()> {
    use arrow::array::{Array, ArrayRef, AsArray, Float32Array, Float64Array};
    use arrow::datatypes::{Field, Float64Type};
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
    let run = |value: ColumnarValue, rows, widen| {
        let return_type = if widen {
            DataType::Float64
        } else {
            value.data_type()
        };
        SparkComparisonFloat { widen }.invoke_with_args(ScalarFunctionArgs {
            arg_fields: vec![Arc::new(Field::new("x", value.data_type(), true))],
            args: vec![value],
            number_rows: rows,
            return_field: Arc::new(Field::new("x", return_type, true)),
            config_options: Arc::new(ConfigOptions::default()),
        })
    };
    let values = [
        Some(-0.0),
        Some(0.0),
        None,
        Some(-0.1),
        Some(0.1),
        Some(f64::INFINITY),
        Some(f64::NEG_INFINITY),
        Some(f64::NAN),
        Some(-f64::NAN),
        Some(f64::from_bits(0x7ff0_0000_0000_0001)),
        Some(f64::from_bits(0xfff0_0000_0000_1234)),
        Some(1e-40),
        Some(-1e-40),
    ];
    let arrays: [ArrayRef; 2] = [
        Arc::new(Float32Array::from_iter(values.map(|v| v.map(|v| v as f32)))),
        Arc::new(Float64Array::from_iter(values)),
    ];
    let mut arrays = arrays.to_vec();
    // Exercise IEEE boundaries and arbitrary bit patterns without a random dependency.
    let mut bits = vec![
        0,
        1,
        0x000f_ffff_ffff_ffff,
        0x0010_0000_0000_0000,
        0x7fef_ffff_ffff_ffff,
        0x7ff0_0000_0000_0000,
        0x7ff0_0000_0000_0001,
        0x7ff8_0000_0000_0000,
        0x7fff_ffff_ffff_ffff,
        0x0000_0001_0000_0000,
        0x007f_ffff_0000_0000,
        0x0080_0000_0000_0000,
        0x7f7f_ffff_0000_0000,
        0x7f80_0000_0000_0000,
        0x7f80_0001_0000_0000,
        0x7fc0_0000_0000_0000,
        0x7fff_ffff_0000_0000,
    ];
    bits.extend(bits.clone().into_iter().map(|v| v | (1 << 63)));
    let mut state = 0x1234_5678_9abc_def0_u64;
    bits.extend((0..8192).map(|_| {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    }));
    arrays.push(Arc::new(Float32Array::from_iter(
        bits.iter()
            .enumerate()
            .map(|(i, &v)| (i % 19 != 18).then_some(f32::from_bits((v >> 32) as u32))),
    )));
    arrays.push(Arc::new(Float64Array::from_iter(
        bits.iter()
            .enumerate()
            .map(|(i, &v)| (i % 19 != 18).then_some(f64::from_bits(v))),
    )));
    for input in arrays {
        for widen in [false, true] {
            for (offset, len) in [(0, input.len()), (1, 10), (1, input.len() - 1), (0, 0)] {
                let input = input.slice(offset, len);
                let output =
                    run(ColumnarValue::Array(input.clone()), len, widen)?.into_array(len)?;
                assert_eq!(
                    output.data_type(),
                    if widen {
                        &DataType::Float64
                    } else {
                        input.data_type()
                    }
                );
                let output = arrow::compute::cast(&output, &DataType::Float64)?;
                let actual = output.as_primitive::<Float64Type>();
                let widened = arrow::compute::cast(&input, &DataType::Float64)?;
                for (i, value) in widened.as_primitive::<Float64Type>().iter().enumerate() {
                    let expected = value.map(|v| {
                        if v.is_nan() {
                            f64::NAN.to_bits()
                        } else if v == 0.0 {
                            0
                        } else {
                            v.to_bits()
                        }
                    });
                    assert_eq!(actual.is_null(i), expected.is_none());
                    if let Some(bits) = expected {
                        assert_eq!(actual.value(i).to_bits(), bits);
                    }
                    let scalar = ScalarValue::try_from_array(&input, i)?;
                    let ColumnarValue::Scalar(result) =
                        run(ColumnarValue::Scalar(scalar), 1, widen)?
                    else {
                        panic!("expected scalar")
                    };
                    assert_eq!(
                        result.data_type(),
                        if widen {
                            DataType::Float64
                        } else {
                            input.data_type().clone()
                        }
                    );
                    let ScalarValue::Float64(result) = result.cast_to(&DataType::Float64)? else {
                        panic!("expected floating scalar")
                    };
                    assert_eq!(result.map(f64::to_bits), expected);
                }
            }
        }
    }
    Ok(())
}

/// Aggregate function input components, excluding the function name.
///
/// Populated by the resolver from `spec::UnresolvedFunction` after resolving spec expressions
/// to DataFusion expressions. Used by aggregate function builders to produce the final
/// DataFusion aggregate expression.
pub struct AggFunctionInput<'a> {
    pub arguments: Vec<expr::Expr>,
    pub distinct: bool,
    pub ignore_nulls: Option<bool>,
    pub filter: Option<Box<expr::Expr>>,
    pub order_by: Vec<expr::Sort>,
    pub function_context: FunctionContextInput<'a>,
}

/// Builds a DataFusion aggregate expression from resolved function components.
///
/// Takes the resolved arguments, modifiers (DISTINCT, FILTER, ORDER BY), and context,
/// and produces a `datafusion_expr::Expr::AggregateFunction` ready for inclusion in the logical plan.
pub(crate) type AggFunction = Arc<dyn Fn(AggFunctionInput) -> PlanResult<expr::Expr> + Send + Sync>;

/// Factory methods for creating `AggFunction`s.
///
/// Provides different ways to build aggregate function handlers:
/// - `default`: Use an existing DataFusion UDAF directly
/// - `custom`: Provide custom logic for building the expression
/// - `unknown`: Placeholder for unimplemented functions
pub(crate) struct AggFunctionBuilder;

impl AggFunctionBuilder {
    /// Converts a DataFusion UDAF factory into an AggFunction.
    ///
    /// Passes all resolved components through directly with no transformation.
    pub fn default<F>(f: F) -> AggFunction
    where
        F: Fn() -> Arc<AggregateUDF> + Send + Sync + 'static,
    {
        Arc::new(move |input| {
            let AggFunctionInput {
                arguments,
                distinct,
                ignore_nulls,
                filter,
                order_by,
                function_context: _function_context,
            } = input;
            let null_treatment = get_null_treatment(ignore_nulls);
            Ok(expr::Expr::AggregateFunction(AggregateFunction {
                func: f(),
                params: AggregateFunctionParams {
                    args: arguments,
                    distinct,
                    filter,
                    order_by,
                    null_treatment,
                },
            }))
        })
    }

    /// Wraps a custom function that builds the aggregate expression with full control.
    ///
    /// Use when the function needs special argument handling that `default` cannot provide.
    pub fn custom<F>(f: F) -> AggFunction
    where
        F: Fn(AggFunctionInput) -> PlanResult<expr::Expr> + Send + Sync + 'static,
    {
        Arc::new(f)
    }

    pub fn unknown(name: &str) -> AggFunction {
        let name = name.to_string();
        Arc::new(move |_| Err(PlanError::todo(format!("function: {name}"))))
    }
}

pub struct WinFunctionInput<'a> {
    pub arguments: Vec<expr::Expr>,
    pub partition_by: Vec<expr::Expr>,
    pub order_by: Vec<expr::Sort>,
    pub window_frame: WindowFrame,
    pub ignore_nulls: Option<bool>,
    pub distinct: bool,
    pub function_context: FunctionContextInput<'a>,
}

pub(crate) type WinFunction = Arc<dyn Fn(WinFunctionInput) -> PlanResult<expr::Expr> + Send + Sync>;

pub(crate) struct WinFunctionBuilder;

impl WinFunctionBuilder {
    pub fn aggregate<F>(f: F) -> WinFunction
    where
        F: Fn() -> Arc<AggregateUDF> + Send + Sync + 'static,
    {
        Arc::new(move |input| {
            let WinFunctionInput {
                arguments,
                partition_by,
                order_by,
                window_frame,
                ignore_nulls,
                distinct,
                function_context: _function_context,
            } = input;
            let null_treatment = get_null_treatment(ignore_nulls);
            Ok(expr::Expr::WindowFunction(Box::new(expr::WindowFunction {
                fun: WindowFunctionDefinition::AggregateUDF(f()),
                params: WindowFunctionParams {
                    args: arguments,
                    partition_by,
                    order_by,
                    window_frame,
                    filter: None,
                    null_treatment,
                    distinct,
                },
            })))
        })
    }

    pub fn window<F>(f: F) -> WinFunction
    where
        F: Fn() -> Arc<WindowUDF> + Send + Sync + 'static,
    {
        Arc::new(move |input| {
            let WinFunctionInput {
                arguments,
                partition_by,
                order_by,
                window_frame,
                ignore_nulls,
                distinct,
                function_context,
            } = input;
            let null_treatment = get_null_treatment(ignore_nulls);
            let win_func_expr = expr::Expr::WindowFunction(Box::new(expr::WindowFunction {
                fun: WindowFunctionDefinition::WindowUDF(f()),
                params: WindowFunctionParams {
                    args: arguments,
                    partition_by,
                    order_by,
                    window_frame,
                    filter: None,
                    null_treatment,
                    distinct,
                },
            }));
            Ok(match win_func_expr.get_type(function_context.schema)? {
                DataType::UInt64 => cast(win_func_expr.clone(), DataType::Int32),
                _ => win_func_expr,
            })
        })
    }

    pub fn custom<F>(f: F) -> WinFunction
    where
        F: Fn(WinFunctionInput) -> PlanResult<expr::Expr> + Send + Sync + 'static,
    {
        Arc::new(f)
    }

    pub fn unknown(name: &str) -> WinFunction {
        let name = name.to_string();
        Arc::new(move |_| Err(PlanError::todo(format!("unknown window function: {name}"))))
    }
}

pub(crate) fn get_null_treatment(ignore_nulls: Option<bool>) -> Option<NullTreatment> {
    match ignore_nulls {
        Some(true) => Some(NullTreatment::IgnoreNulls),
        Some(false) => Some(NullTreatment::RespectNulls),
        None => None,
    }
}

pub(crate) fn get_arguments_and_null_treatment(
    args: Vec<expr::Expr>,
    ignore_nulls: Option<bool>,
) -> PlanResult<(Vec<expr::Expr>, Option<NullTreatment>)> {
    if args.len() == 1 {
        let expr = args.one()?;
        Ok((vec![expr], get_null_treatment(ignore_nulls)))
    } else if args.len() == 2 {
        if ignore_nulls.is_some() {
            return Err(PlanError::invalid(
                "arguments conflict with IGNORE NULLS clause",
            ));
        }
        let (expr, ignore_nulls) = args.two()?;
        let null_treatment = match ignore_nulls {
            expr::Expr::Literal(ScalarValue::Boolean(Some(ignore_nulls)), _metadata) => {
                if ignore_nulls {
                    Some(NullTreatment::IgnoreNulls)
                } else {
                    Some(NullTreatment::RespectNulls)
                }
            }
            _ => {
                return Err(PlanError::invalid(
                    "requires a boolean literal as the second argument",
                ));
            }
        };
        Ok((vec![expr], null_treatment))
    } else {
        Err(PlanError::invalid("requires 1 or 2 arguments"))
    }
}

pub(crate) fn hll_args_with_default_lg(
    arguments: Vec<expr::Expr>,
    function_name: &str,
) -> PlanResult<Vec<expr::Expr>> {
    match arguments.len() {
        1 => {
            let value = arguments.one()?;
            Ok(vec![value, lit(DEFAULT_HLL_LG_CONFIG_K)])
        }
        2 => {
            let (value, lg_config_k) = arguments.two()?;
            Ok(vec![value, cast(lg_config_k, DataType::Int32)])
        }
        count => Err(PlanError::invalid(format!(
            "{function_name} requires 1 or 2 arguments, got {count}"
        ))),
    }
}

pub(crate) fn hll_union_args_with_default_allow_different_lg(
    arguments: Vec<expr::Expr>,
) -> PlanResult<Vec<expr::Expr>> {
    match arguments.len() {
        1 => {
            let value = arguments.one()?;
            Ok(vec![value, lit(false)])
        }
        2 => {
            let (value, allow_different_lg_config_k) = arguments.two()?;
            Ok(vec![
                value,
                cast(allow_different_lg_config_k, DataType::Boolean),
            ])
        }
        count => Err(PlanError::invalid(format!(
            "hll_union_agg requires 1 or 2 arguments, got {count}"
        ))),
    }
}

pub(crate) fn count_min_sketch_args(arguments: Vec<expr::Expr>) -> PlanResult<Vec<expr::Expr>> {
    match arguments.len() {
        4 => {
            let (value, eps, confidence, seed) = arguments.four()?;
            // Spark requires `eps` and `confidence` to be DOUBLE literals and rejects
            // other types (including DECIMAL/FLOAT). Pass them through unchanged so the
            // aggregate's type validation enforces the same rule, instead of casting and
            // silently accepting types Spark rejects.
            Ok(vec![value, eps, confidence, seed])
        }
        count => Err(PlanError::invalid(format!(
            "count_min_sketch requires 4 arguments, got {count}"
        ))),
    }
}

pub(crate) fn theta_args_with_default_lg(
    arguments: Vec<expr::Expr>,
    function_name: &str,
) -> PlanResult<Vec<expr::Expr>> {
    match arguments.len() {
        1 => {
            let value = arguments.one()?;
            Ok(vec![value, lit(DEFAULT_THETA_LG_NOM_ENTRIES)])
        }
        2 => {
            let (value, lg_nom_entries) = arguments.two()?;
            Ok(vec![value, cast(lg_nom_entries, DataType::Int32)])
        }
        count => Err(PlanError::invalid(format!(
            "{function_name} requires 1 or 2 arguments, got {count}"
        ))),
    }
}

// TODO: Match Catalyst constant folding and NullPropagation before extracting opaque
//  Variant scalar calls, so a foldable NULL can eliminate them.
pub fn expr_contains_spark_cast_to_variant(body: &expr::Expr) -> PlanResult<bool> {
    Ok(body.exists(|expression| {
        Ok(matches!(
            expression,
            expr::Expr::ScalarFunction(function)
                if function.func.inner().is::<SparkCastToVariant>()
        ))
    })?)
}

#[test]
fn comparison_float_simplifies_only_canonical_inputs() -> datafusion_common::Result<()> {
    use arrow::array::{
        ArrayRef, Decimal32Array, Decimal64Array, Decimal128Array, Decimal256Array, Float32Array,
        Float64Array, Int64Array, UInt64Array,
    };
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::physical_expr::{create_physical_expr, execution_props::ExecutionProps};
    use datafusion_common::{DFSchema, scalar::ScalarValue};
    use datafusion_expr::col;
    use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};

    let mut arrays: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![
            Some(i64::MIN),
            Some(-1),
            Some(0),
            Some(1),
            Some(i64::MAX),
            None,
        ])),
        Arc::new(UInt64Array::from(vec![
            Some(0),
            Some(1),
            Some(u64::MAX),
            None,
        ])),
        Arc::new(Float32Array::from(vec![
            Some(-0.0),
            Some(f32::from_bits(0xffc00001)),
            Some(1.5),
            None,
        ])),
        Arc::new(Float64Array::from(vec![
            Some(-0.0),
            Some(f64::from_bits(0xfff8000000000001)),
            Some(1.5),
            None,
        ])),
    ];
    for data_type in [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
    ] {
        arrays.push(arrow::compute::cast(arrays[0].as_ref(), &data_type)?);
    }
    for scale in [-128, 0, 9] {
        arrays.push(Arc::new(
            Decimal32Array::from(vec![
                Some(-999_999_999),
                Some(-1),
                Some(0),
                Some(1),
                Some(999_999_999),
                None,
            ])
            .with_precision_and_scale(9, scale)?,
        ));
    }
    for scale in [-128, 0, 18] {
        arrays.push(Arc::new(
            Decimal64Array::from(vec![
                Some(-999_999_999_999_999_999),
                Some(-1),
                Some(0),
                Some(1),
                Some(999_999_999_999_999_999),
                None,
            ])
            .with_precision_and_scale(18, scale)?,
        ));
    }
    for scale in [-128, 0, 38] {
        let max = 10_i128.pow(38) - 1;
        arrays.push(Arc::new(
            Decimal128Array::from(vec![
                Some(-max),
                Some(-1),
                Some(0),
                Some(1),
                Some(max),
                None,
            ])
            .with_precision_and_scale(38, scale)?,
        ));
    }
    for scale in [-128, 0, 76] {
        let max = "9".repeat(76).parse::<arrow::datatypes::i256>().unwrap();
        arrays.push(Arc::new(
            Decimal256Array::from(vec![
                Some(-max),
                Some(arrow::datatypes::i256::from_i128(-1)),
                Some(arrow::datatypes::i256::ZERO),
                Some(arrow::datatypes::i256::ONE),
                Some(max),
                None,
            ])
            .with_precision_and_scale(76, scale)?,
        ));
    }
    for input in arrays {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "v",
            input.data_type().clone(),
            true,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![input.clone()])?;
        let schema = Arc::new(DFSchema::try_from(schema)?);
        let info = SimplifyContext::builder()
            .with_schema(schema.clone())
            .build();
        for data_type in [DataType::Float32, DataType::Float64] {
            for safe in [false, true] {
                let cast = if safe {
                    try_cast(col("v"), data_type.clone())
                } else {
                    cast(col("v"), data_type.clone())
                };
                for nested in [false, true] {
                    let arg = if nested {
                        ScalarUDF::from(SparkComparisonFloat { widen: false })
                            .call(vec![cast.clone()])
                    } else {
                        cast.clone()
                    };
                    for widen in [false, true] {
                        let function = ScalarUDF::from(SparkComparisonFloat { widen });
                        let original = function.call(vec![arg.clone()]);
                        let result = function.simplify(vec![arg.clone()], &info)?;
                        let should_simplify = nested
                            || input.data_type().is_integer()
                            || (input.data_type().is_decimal() && data_type == DataType::Float64);
                        assert_eq!(
                            matches!(&result, ExprSimplifyResult::Simplified(_)),
                            should_simplify
                        );
                        let simplified = match result {
                            ExprSimplifyResult::Simplified(e) => e,
                            ExprSimplifyResult::Original(args) => function.call(args),
                        };
                        assert_eq!(original.get_type(&schema)?, simplified.get_type(&schema)?);
                        assert_eq!(original.nullable(&schema)?, simplified.nullable(&schema)?);
                        let evaluate = |e: &expr::Expr| {
                            create_physical_expr(e, &schema, &ExecutionProps::new())?
                                .evaluate(&batch)?
                                .into_array(batch.num_rows())
                        };
                        let a = evaluate(&original)?;
                        let b = evaluate(&simplified)?;
                        for i in 0..batch.num_rows() {
                            let bits =
                                |array: &ArrayRef| -> datafusion_common::Result<Option<u64>> {
                                    let ScalarValue::Float64(value) =
                                        ScalarValue::try_from_array(array, i)?
                                            .cast_to(&DataType::Float64)?
                                    else {
                                        unreachable!()
                                    };
                                    Ok(value.map(f64::to_bits))
                                };
                            assert_eq!(
                                bits(&a)?,
                                bits(&b)?,
                                "input={}, target={data_type}, safe={safe}, nested={nested}, widen={widen}, row={i}",
                                input.data_type()
                            );
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

#[test]
fn ansi_float_integer_predicates_preserve_the_single_precision_boundary() -> PlanResult<()> {
    use arrow::array::{AsArray, Float32Array, Int64Array};
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::optimizer::simplify_expressions::ExprSimplifier;
    use datafusion::physical_expr::create_physical_expr;
    use datafusion_common::DFSchema;
    use datafusion_expr::{col, execution_props::ExecutionProps, simplify::SimplifyContext};

    let schema = Arc::new(Schema::new(vec![
        Field::new("f", DataType::Float32, true),
        Field::new("i", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Float32Array::from(vec![
                Some(16777216.),
                Some(-16777216.),
                Some(16777216.),
                Some(1.5),
                Some(-0.),
                None,
                Some(1.),
            ])),
            Arc::new(Int64Array::from(vec![
                Some(16777217),
                Some(-16777217),
                Some(16777216),
                Some(1),
                Some(0),
                Some(1),
                None,
            ])),
        ],
    )?;
    let schema = Arc::new(DFSchema::try_from(schema)?);
    for ansi_mode in [false, true] {
        let expected = vec![
            Some(!ansi_mode),
            Some(!ansi_mode),
            Some(true),
            Some(false),
            Some(true),
            None,
            None,
        ];
        for (left, right) in [(col("f"), col("i")), (col("i"), col("f"))] {
            for expression in [
                spark_binary_expr(
                    left.clone(),
                    Operator::Eq,
                    right.clone(),
                    &schema,
                    ansi_mode,
                )?,
                spark_in_list(left, vec![right], false, &schema, ansi_mode)?,
            ] {
                let simplifier = ExprSimplifier::new(
                    SimplifyContext::builder()
                        .with_schema(schema.clone())
                        .build(),
                );
                let expression = simplifier.coerce(expression, &schema)?;
                let actual = create_physical_expr(&expression, &schema, &ExecutionProps::new())?
                    .evaluate(&batch)?
                    .into_array(batch.num_rows())?;
                assert_eq!(
                    actual.as_boolean().iter().collect::<Vec<_>>(),
                    expected,
                    "ANSI={ansi_mode}"
                );
            }
        }
    }
    Ok(())
}

#[test]
fn exact_integer_predicates_match_explicit_double_coercion() -> PlanResult<()> {
    use arrow::array::{Array, ArrayRef, Float32Array, Int8Array, Int16Array};
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::optimizer::simplify_expressions::ExprSimplifier;
    use datafusion::physical_expr::create_physical_expr;
    use datafusion_common::DFSchema;
    use datafusion_expr::{col, execution_props::ExecutionProps, simplify::SimplifyContext};

    let floats = vec![
        Some(f32::NEG_INFINITY),
        Some(-9223372036854775808.),
        Some(-16777218.),
        Some(-16777216.),
        Some(-32768.),
        Some(-128.),
        Some(-1.5),
        Some(-0.),
        Some(0.),
        Some(1.),
        Some(1.5),
        Some(127.),
        Some(32767.),
        Some(16777216.),
        Some(16777218.),
        Some(33554432.),
        Some(33554436.),
        Some(9007199254740992.),
        Some(9223372036854775808.),
        Some(f32::INFINITY),
        Some(f32::NAN),
        None,
    ];
    let schema = Arc::new(Schema::new(vec![
        Field::new("f", DataType::Float32, true),
        Field::new("tiny", DataType::Int8, true),
        Field::new("small", DataType::Int16, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Float32Array::from(floats.clone())),
            Arc::new(Int8Array::from_iter((0..floats.len()).map(|i| {
                (i % 5 != 0).then_some(if i % 2 == 0 { i8::MIN } else { i8::MAX })
            }))),
            Arc::new(Int16Array::from_iter((0..floats.len()).map(|i| {
                (i % 7 != 0).then_some(if i % 2 == 0 { i16::MIN } else { i16::MAX })
            }))),
        ],
    )?;
    let schema = Arc::new(DFSchema::try_from(schema)?);
    let evaluate = |expression: expr::Expr| -> PlanResult<ArrayRef> {
        let simplifier = ExprSimplifier::new(
            SimplifyContext::builder()
                .with_schema(schema.clone())
                .build(),
        );
        let expression = simplifier.coerce(expression, &schema)?;
        Ok(
            create_physical_expr(&expression, &schema, &ExecutionProps::new())?
                .evaluate(&batch)?
                .into_array(batch.num_rows())?,
        )
    };
    let mut integers = vec![
        col("tiny"),
        col("small"),
        lit(ScalarValue::Int32(None)),
        lit(ScalarValue::Int64(None)),
    ];
    for value in [
        0_i64,
        1,
        -1,
        16777215,
        16777216,
        16777217,
        16777218,
        -16777217,
        33554431,
        33554432,
        33554434,
        33554436,
        9007199254740991,
        9007199254740992,
        9007199254740993,
        9007199254740994,
        i64::MIN,
        i64::MIN + 1,
        i64::MAX,
    ] {
        integers.push(lit(value));
        if let Ok(value) = i32::try_from(value) {
            integers.push(lit(value));
        }
    }
    for integer in integers {
        for (left, right) in [(col("f"), integer.clone()), (integer, col("f"))] {
            let wide = |expr: expr::Expr| -> PlanResult<expr::Expr> {
                Ok(if expr.get_type(&schema)? == DataType::Float32 {
                    ScalarUDF::from(SparkComparisonFloat { widen: true }).call(vec![expr])
                } else {
                    cast(expr, DataType::Float64)
                })
            };
            let (wide_left, wide_right) = (wide(left.clone())?, wide(right.clone())?);
            for op in [
                Operator::Eq,
                Operator::NotEq,
                Operator::Lt,
                Operator::LtEq,
                Operator::Gt,
                Operator::GtEq,
                Operator::IsDistinctFrom,
                Operator::IsNotDistinctFrom,
            ] {
                let candidate = spark_binary_expr(left.clone(), op, right.clone(), &schema, true)?;
                let reference = expr::Expr::BinaryExpr(BinaryExpr::new(
                    Box::new(wide_left.clone()),
                    op,
                    Box::new(wide_right.clone()),
                ));
                assert_eq!(
                    evaluate(candidate)?.to_data(),
                    evaluate(reference)?.to_data()
                );
            }
            for negated in [false, true] {
                let candidate =
                    spark_in_list(left.clone(), vec![right.clone()], negated, &schema, true)?;
                let reference = expr::Expr::InList(expr::InList::new(
                    Box::new(wide_left.clone()),
                    vec![wide_right.clone()],
                    negated,
                ));
                assert_eq!(
                    evaluate(candidate)?.to_data(),
                    evaluate(reference)?.to_data()
                );
            }
        }
    }
    Ok(())
}
