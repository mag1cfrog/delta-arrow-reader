//! Constant evaluation respects UDF folding permission and Spark nested declarations.

use std::sync::{Arc, LazyLock};

use arrow::array::{Int32Array, StructArray};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::config::ConfigOptions;
use datafusion::common::{DFSchema, Result, ScalarValue, exec_err};
use datafusion::logical_expr::simplify::SimplifyContext;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ExprSchemable, LogicalPlanBuilder, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, Volatility, cast, col, create_udf, lit,
};
use datafusion::optimizer::AnalyzerRule;
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use sail_function::scalar::array::spark_array::SparkArray;
use sail_function::scalar::collection::spark_concat::SparkConcat;
use sail_function::scalar::spark_cast_string_to_integer::{
    SparkCastStringToInteger, StringIntegerCastMode,
};
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
fn reachable_opt_out_call_keeps_native_child_folding() -> Result<()> {
    use std::sync::atomic::{AtomicUsize, Ordering};

    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let allowed = create_udf(
        "reachable_child",
        vec![DataType::Int32],
        DataType::Int32,
        Volatility::Immutable,
        Arc::new(move |args| {
            observed.fetch_add(1, Ordering::SeqCst);
            Ok(args[0].clone())
        }),
    );
    let retained = ScalarUDF::from(RetainConstant);
    for spark in [false, true] {
        calls.store(0, Ordering::SeqCst);
        let child = allowed.call(vec![lit(1_i32) + lit(2_i32)]);
        assert_eq!(
            simplify(retained.call(vec![child]), spark)?,
            retained.call(vec![lit(3_i32)])
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
    Ok(())
}

#[test]
fn spark_closed_conditionals_evaluate_permitted_producers_once() -> Result<()> {
    use datafusion::common::tree_node::TreeNode;
    use datafusion::functions::expr_fn::coalesce;
    use datafusion::logical_expr::expr_fn::when;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let producer = create_udf(
        "conditional_counted_producer",
        vec![DataType::Int32],
        DataType::Int32,
        Volatility::Immutable,
        Arc::new(move |args| {
            observed.fetch_add(1, Ordering::SeqCst);
            Ok(args[0].clone())
        }),
    );
    let failure = create_udf(
        "conditional_counted_failure",
        vec![DataType::Int32],
        DataType::Int32,
        Volatility::Immutable,
        Arc::new(|_| exec_err!("deferred generic failure after permitted work")),
    );
    let child = producer.call(vec![lit(7_i32)]);
    let failing_child = child.clone() + failure.call(vec![lit(0_i32)]);
    for spark in [false, true] {
        for (form, expr, fails) in [
            (
                "case",
                when(lit(true), child.clone()).otherwise(lit(0_i32))?,
                false,
            ),
            (
                "coalesce",
                coalesce(vec![lit(ScalarValue::Int32(None)), child.clone()]),
                false,
            ),
            (
                "nested_case",
                when(
                    lit(true),
                    when(lit(true), child.clone()).otherwise(lit(0_i32))?,
                )
                .otherwise(lit(0_i32))?,
                false,
            ),
            (
                "failed_case",
                when(lit(true), failing_child.clone()).otherwise(lit(0_i32))?,
                true,
            ),
            (
                "failed_coalesce",
                coalesce(vec![failing_child.clone(), lit(0_i32)]),
                true,
            ),
            (
                "failed_nested_case",
                when(
                    lit(true),
                    when(lit(true), failing_child.clone()).otherwise(lit(0_i32))?,
                )
                .otherwise(lit(0_i32))?,
                true,
            ),
        ] {
            calls.store(0, Ordering::SeqCst);
            let result = simplify(expr, spark)?;
            let count = calls.load(Ordering::SeqCst);
            eprintln!("CONDITIONAL_FOLD_CALLBACK_COUNT form={form} spark={spark} calls={count}");
            assert_eq!(count, 1, "{form}, Spark marker={spark}");
            if fails {
                assert!(
                    result.exists(|expr| Ok(matches!(expr, Expr::ScalarFunction(function)
                    if function.func.name() == "conditional_counted_failure")))?
                );
            } else {
                assert_eq!(result, lit(7_i32), "{form}, Spark marker={spark}");
            }
        }
    }
    Ok(())
}

#[test]
fn spark_whole_nested_folding_retains_declarations_and_values() -> Result<()> {
    for spark in [false, true] {
        // Spark's cast declares nullable even when its literal input is valid.
        let child =
            SparkCastStringToInteger::try_new(DataType::Int32, StringIntegerCastMode::Ansi)?
                .into_expr(lit("3"));
        assert!(child.nullable(&DFSchema::empty())?);
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
        for (parent, expected) in nested_parents(child)
            .into_iter()
            .zip([expected_array, expected_struct])
        {
            let folded = simplify(parent.clone(), spark)?;
            assert_eq!(
                folded.get_type(&DFSchema::empty())?,
                expected.get_type(&DFSchema::empty())?,
                "Spark marker={spark}, original={parent:?}"
            );
            assert_eq!(
                folded, expected,
                "Spark marker={spark}, original={parent:?}"
            );
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

#[test]
fn concat_null_waits_for_constant_operands_and_honors_folding_permission() -> Result<()> {
    use datafusion::logical_expr::simplify::ExprSimplifyResult;
    let concat = SparkConcat::new();
    let info = SimplifyContext::builder().build();
    let null = lit(ScalarValue::Utf8(None));
    let pending = cast(lit(1_i32) + lit(2_i32), DataType::Utf8);
    for args in [
        vec![null.clone(), pending.clone()],
        vec![pending, null.clone()],
    ] {
        match concat.simplify(args.clone(), &info)? {
            ExprSimplifyResult::Original(actual) => assert_eq!(actual, args),
            other => return exec_err!("constant operand was discarded: {other:?}"),
        }
    }
    let stable = create_udf(
        "concat_stable",
        vec![DataType::Int32],
        DataType::Int32,
        Volatility::Stable,
        Arc::new(|args| Ok(args[0].clone())),
    );
    assert!(matches!(
        concat.simplify(vec![null.clone(), stable.call(vec![lit(3_i32)])], &info)?,
        ExprSimplifyResult::Original(_)
    ));
    let volatile = create_udf(
        "concat_volatile",
        vec![DataType::Int32],
        DataType::Int32,
        Volatility::Volatile,
        Arc::new(|args| Ok(args[0].clone())),
    );
    for arg in [
        lit("ready"),
        ScalarUDF::from(RetainConstant).call(vec![lit(3_i32)]),
        volatile.call(vec![lit(3_i32)]),
    ] {
        match concat.simplify(vec![null.clone(), arg], &info)? {
            ExprSimplifyResult::Simplified(actual) => assert_eq!(actual, null),
            other => return exec_err!("existing NULL fold changed: {other:?}"),
        }
    }
    // Arguments may fold when an opt-out call remains reachable. A known NULL
    // CONCAT can discard the opaque operand without visiting those arguments.
    for function in [
        ScalarUDF::from(RetainConstant),
        ScalarUDF::from(RetainConstant).with_aliases(["retained_alias"]),
    ] {
        for child in [lit(1_i32) + lit(2_i32), lit(1_i32) / lit(0_i32)] {
            let arg = function.call(vec![child]);
            for args in [vec![null.clone(), arg.clone()], vec![arg, null.clone()]] {
                match concat.simplify(args, &info)? {
                    ExprSimplifyResult::Simplified(actual) => assert_eq!(actual, null),
                    other => return exec_err!("opaque NULL operand was retained: {other:?}"),
                }
            }
        }
    }
    // A folding-disabled descendant keeps the entire native extension context
    // on its old NULL path, even beside an independent pending constant.
    let opaque = stable.call(vec![ScalarUDF::from(RetainConstant).call(vec![lit(1_i32)])]);
    let required = cast(lit(1_i32) / lit(0_i32), DataType::Utf8);
    for args in [
        vec![null.clone(), opaque.clone(), required.clone()],
        vec![required, opaque, null.clone()],
    ] {
        match concat.simplify(args, &info)? {
            ExprSimplifyResult::Simplified(actual) => assert_eq!(actual, null),
            other => return exec_err!("opaque native NULL context changed: {other:?}"),
        }
    }
    assert!(matches!(
        concat.simplify(
            vec![null.clone(), volatile.call(vec![lit(1_i32) + lit(2_i32)])],
            &info
        )?,
        ExprSimplifyResult::Original(_)
    ));
    let array = ScalarUDF::from(SparkArray::new()).call(vec![lit(1_i32) + lit(2_i32)]);
    let array_null = lit(ScalarValue::try_from(&array.get_type(&DFSchema::empty())?)?);
    for args in [
        vec![array_null.clone(), array.clone()],
        vec![array, array_null.clone()],
    ] {
        match concat.simplify(args, &info)? {
            ExprSimplifyResult::Simplified(actual) => assert_eq!(actual, array_null),
            other => return exec_err!("ARRAY_CONCAT NULL rule changed: {other:?}"),
        }
    }
    Ok(())
}

#[test]
fn concat_constant_descendants_keep_native_conditional_reachability() -> Result<()> {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use datafusion::logical_expr::simplify::ExprSimplifyResult;
    let info = SimplifyContext::builder()
        .with_schema(Arc::new(DFSchema::try_from(Schema::new(vec![
            Field::new("s", DataType::Utf8, false),
            Field::new("flag", DataType::Boolean, false),
        ]))?))
        .build();
    let concat = SparkConcat::new();
    let null = lit(ScalarValue::Utf8(None));
    let operand_calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&operand_calls);
    let ordinary = create_udf(
        "concat_descendant",
        vec![DataType::Utf8, DataType::Int32],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
            exec_err!("CONCAT guard evaluated an operand")
        }),
    );
    let pending = cast(lit(1_i32) / lit(0_i32), DataType::Int32);
    let argument = ordinary.call(vec![col("s"), pending.clone()]);
    for args in [
        vec![null.clone(), argument.clone()],
        vec![argument, null.clone()],
    ] {
        match concat.simplify(args.clone(), &info)? {
            ExprSimplifyResult::Original(actual) => assert_eq!(actual, args),
            other => return exec_err!("constant descendant was discarded: {other:?}"),
        }
    }
    let conditional =
        datafusion::logical_expr::expr_fn::when(col("flag"), pending).otherwise(lit(1_i32))?;
    for position in [lit(1_i32), conditional] {
        match concat.simplify(
            vec![null.clone(), ordinary.call(vec![col("s"), position])],
            &info,
        )? {
            ExprSimplifyResult::Simplified(actual) => assert_eq!(actual, null),
            other => return exec_err!("runtime operand pruning changed: {other:?}"),
        }
    }
    assert_eq!(operand_calls.load(Ordering::SeqCst), 0);
    Ok(())
}

#[tokio::test]
async fn substring_null_length_keeps_unused_opt_out_descendants_uninvoked() -> Result<()> {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use datafusion::execution::SessionStateBuilder;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;
    use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};

    let session = SessionContext::new_with_state(
        configure_spark_sql(SessionStateBuilder::new().with_default_features()).build(),
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    session.register_udf(ScalarUDF::from(RetainConstant));
    session.register_udf(create_udf(
        "substring_unused_producer",
        vec![DataType::Int32],
        DataType::Int32,
        Volatility::Immutable,
        Arc::new(move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
            exec_err!("unused substring producer was invoked")
        }),
    ));
    for ansi_mode in [true, false] {
        for length_type in ["INT", "STRING"] {
            let sql = format!(
                "SELECT substring('abcdef',retain_constant(substring_unused_producer(1)),CAST(NULL AS {length_type})) AS r"
            );
            let query = plan_query(
                &session,
                &sql,
                &SparkSqlOptions {
                    ansi_mode,
                    ..Default::default()
                },
            )
            .await?;
            let physical = query.create_physical_plan(&session).await?;
            assert_eq!(calls.load(Ordering::SeqCst), 0, "{sql}");
            let batches = collect(physical, session.task_ctx()).await?;
            assert_eq!(
                batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
                1
            );
            for batch in batches {
                assert_eq!(batch.column(0).null_count(), batch.num_rows(), "{sql}");
            }
            assert_eq!(calls.load(Ordering::SeqCst), 0, "{sql}");
        }
    }
    Ok(())
}

#[tokio::test]
async fn mixed_numeric_array_preparation_preserves_inherited_callback_limit() -> Result<()> {
    use datafusion::execution::SessionStateBuilder;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use delta_arrow_reader_spark_sql::{SparkSqlOptions, configure_spark_sql, plan_query};
    use std::sync::atomic::{AtomicUsize, Ordering};

    static CALLS: AtomicUsize = AtomicUsize::new(0);
    #[derive(Debug, PartialEq, Eq, Hash)]
    struct CountedOptOut;
    impl ScalarUDFImpl for CountedOptOut {
        fn name(&self) -> &str {
            "mixed_counted_producer"
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
            CALLS.fetch_add(1, Ordering::SeqCst);
            Ok(args.args[0].clone())
        }
    }
    let session = SessionContext::new_with_state(
        configure_spark_sql(
            SessionStateBuilder::new()
                .with_default_features()
                .with_config(
                    SessionConfig::new()
                        .with_batch_size(1)
                        .with_target_partitions(1),
                ),
        )
        .build(),
    );
    session.register_udf(ScalarUDF::from(CountedOptOut));
    let sql = "SELECT (1 DIV 0)+ARRAY(mixed_counted_producer(7))[1] AS r";
    CALLS.store(0, Ordering::SeqCst);
    let query = plan_query(&session, sql, &SparkSqlOptions::default()).await?;
    let (phase, error) = match query.create_physical_plan(&session).await {
        Err(error) => ("create_physical_plan", error),
        Ok(physical) => (
            "collect",
            collect(physical, session.task_ctx())
                .await
                .err()
                .ok_or_else(|| {
                    datafusion::common::DataFusionError::Internal(
                        "required numeric error disappeared".into(),
                    )
                })?,
        ),
    };
    let count = CALLS.load(Ordering::SeqCst);
    eprintln!(
        "MIXED_NUMERIC_ARRAY_CALLBACK_OBSERVATION {}",
        serde_json::json!({
            "sql":sql,"phase":phase,"error":error.to_string(),"calls":count,
            "supports_constant_folding":false,"volatility":"Immutable"
        })
    );
    assert_eq!(phase, "create_physical_plan");
    assert!(
        error
            .to_string()
            .ends_with("Arrow error: Divide by zero error"),
        "{error}"
    );
    // The accepted numeric precheck invoked this opt-out twice; #893 owns that inherited policy.
    assert!(
        count <= 2,
        "existing numeric callback count increased: {count}"
    );
    Ok(())
}
