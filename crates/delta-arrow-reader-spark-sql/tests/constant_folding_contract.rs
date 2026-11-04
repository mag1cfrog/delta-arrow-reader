//! Constant evaluation respects UDF folding permission and Spark nested declarations.

use std::sync::{Arc, LazyLock};

use arrow::array::{Int32Array, StructArray};
use arrow::datatypes::{DataType, Field};
use datafusion::common::config::ConfigOptions;
use datafusion::common::{Result, ScalarValue, exec_err};
use datafusion::logical_expr::simplify::SimplifyContext;
use datafusion::logical_expr::{
    ColumnarValue, Expr, LogicalPlanBuilder, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, Volatility, cast, create_udf, lit, try_cast,
};
use datafusion::optimizer::AnalyzerRule;
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use sail_function::scalar::array::spark_array::SparkArray;
use sail_function::scalar::struct_function::StructFunction;
use sail_plan::expression_errors::{PrepareSparkExpressions, SparkSqlExpressionSemantics};

#[derive(Debug, PartialEq, Eq, Hash)]
struct RetainConstant;

impl ScalarUDFImpl for RetainConstant {
    fn name(&self) -> &str {
        "retain_constant"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: LazyLock<Signature> =
            LazyLock::new(|| Signature::exact(vec![DataType::Int32], Volatility::Immutable));
        &SIGNATURE
    }

    fn supports_constant_folding(&self) -> bool {
        false
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        Ok(args.args[0].clone())
    }
}

fn options(spark: bool) -> ConfigOptions {
    let mut options = ConfigOptions::default();
    if spark {
        options
            .extensions
            .insert(SparkSqlExpressionSemantics::default());
    }
    options
}

fn simplify(expr: Expr, spark: bool) -> Result<Expr> {
    let context = SimplifyContext::builder()
        .with_config_options(Arc::new(options(spark)))
        .build();
    ExprSimplifier::new(context).simplify(expr)
}

fn nested_parents(child: Expr) -> [Expr; 2] {
    [
        ScalarUDF::from(SparkArray::new()).call(vec![child.clone()]),
        ScalarUDF::from(StructFunction::new(vec!["x".into()])).call(vec![child]),
    ]
}

#[test]
fn immutable_folding_opt_out_and_aliases_preserve_simplifiable_children() -> Result<()> {
    let default = create_udf(
        "default_fold",
        vec![DataType::Int32],
        DataType::Int32,
        Volatility::Immutable,
        Arc::new(|args| Ok(args[0].clone())),
    );
    let retained = ScalarUDF::from(RetainConstant);
    let volatile = create_udf(
        "volatile_identity",
        vec![DataType::Int32],
        DataType::Int32,
        Volatility::Volatile,
        Arc::new(|args| Ok(args[0].clone())),
    );
    assert!(default.inner().supports_constant_folding());
    assert!(!retained.call(vec![lit(3_i32)]).is_volatile());
    assert!(volatile.call(vec![lit(3_i32)]).is_volatile());
    for spark in [false, true] {
        assert_eq!(
            simplify(default.call(vec![lit(1_i32) + lit(2_i32)]), spark)?,
            lit(3_i32)
        );
        for function in [
            retained.clone(),
            retained.clone().with_aliases(["retained_alias"]),
        ] {
            assert!(!function.inner().supports_constant_folding());
            let expr = function.call(vec![lit(1_i32) + lit(2_i32)]);
            let expected = function.call(vec![lit(3_i32)]);
            assert_eq!(simplify(expr.clone(), spark)?, expected);
            assert_eq!(
                simplify(expr + lit(1_i32), spark)?,
                expected.clone() + lit(1_i32)
            );
            for (parent, expected_parent) in
                nested_parents(function.call(vec![lit(1_i32) + lit(2_i32)]))
                    .into_iter()
                    .zip(nested_parents(expected))
            {
                assert_eq!(simplify(parent, spark)?, expected_parent);
            }
            for parent in nested_parents(function.call(vec![cast(lit("bad"), DataType::Int32)])) {
                assert!(simplify(parent, spark).is_err());
            }
        }
        for (parent, expected) in nested_parents(volatile.call(vec![lit(1_i32) + lit(2_i32)]))
            .into_iter()
            .zip(nested_parents(volatile.call(vec![lit(3_i32)])))
        {
            assert_eq!(simplify(parent, spark)?, expected);
        }
    }
    Ok(())
}

#[test]
fn spark_whole_nested_folding_retains_declarations_and_values() -> Result<()> {
    for spark in [false, true] {
        let expected_array = lit(ScalarValue::List(ScalarValue::new_list(
            &[ScalarValue::Int32(Some(3))],
            &DataType::Int32,
            spark,
        )));
        let expected_struct = lit(ScalarValue::Struct(Arc::new(StructArray::try_new(
            vec![Field::new("x", DataType::Int32, spark)].into(),
            vec![Arc::new(Int32Array::from(vec![3]))],
            None,
        )?)));
        for (parent, expected) in nested_parents(try_cast(lit("3"), DataType::Int32))
            .into_iter()
            .zip([expected_array, expected_struct])
        {
            let folded = simplify(parent, spark)?;
            assert_eq!(folded, expected);
        }
    }
    Ok(())
}

#[test]
fn spark_struct_preparation_honors_descendant_folding_opt_out() -> Result<()> {
    let retained = ScalarUDF::from(RetainConstant).with_aliases(["retained_alias"]);
    for expr in nested_parents(retained.call(vec![lit(3_i32)])) {
        let struct_ancestor =
            ScalarUDF::from(StructFunction::new(vec!["outer".into()])).call(vec![expr]);
        let plan = LogicalPlanBuilder::empty(true)
            .project(vec![struct_ancestor.alias("r")])?
            .build()?;
        let prepared = PrepareSparkExpressions.analyze(plan.clone(), &options(true))?;
        assert_eq!(prepared, plan);
    }
    Ok(())
}

#[test]
fn failed_speculative_nested_folding_keeps_required_and_runtime_errors() -> Result<()> {
    let runtime_error = create_udf(
        "runtime_error",
        vec![DataType::Int32],
        DataType::Int32,
        Volatility::Immutable,
        Arc::new(|_| exec_err!("deferred UDF failure")),
    );
    for spark in [false, true] {
        for parent in nested_parents(cast(lit("bad"), DataType::Int32)) {
            assert!(simplify(parent, spark).is_err());
        }
        for (parent, expected) in nested_parents(runtime_error.call(vec![lit(1_i32) + lit(2_i32)]))
            .into_iter()
            .zip(nested_parents(runtime_error.call(vec![lit(3_i32)])))
        {
            assert_eq!(simplify(parent, spark)?, expected);
        }
        let bad_boolean = cast(lit("bad"), DataType::Boolean);
        assert_eq!(
            simplify(lit(false).and(bad_boolean.clone()), spark)?,
            lit(false)
        );
        assert!(simplify(bad_boolean.and(lit(false)), spark).is_err());
    }
    Ok(())
}
