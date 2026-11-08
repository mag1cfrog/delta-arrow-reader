// Local Spark planning adapter. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md.
use std::sync::Arc;

use datafusion::arrow::array::AsArray;
use datafusion::arrow::compute::{cast as arrow_cast, filter_record_batch};
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::functions::expr_fn;
use datafusion::logical_expr::execution_props::ExecutionProps;
use datafusion::optimizer::optimize_projections::OptimizeProjections;
use datafusion::optimizer::propagate_empty_relation::PropagateEmptyRelation;
use datafusion::optimizer::push_down_filter::PushDownFilter;
use datafusion::optimizer::simplify_expressions::{ExprSimplifier, prune_null};
use datafusion::optimizer::{AnalyzerRule, OptimizerContext, OptimizerRule};
use datafusion::physical_expr::create_physical_expr;
use datafusion_common::config::{ConfigExtension, ConfigOptions};
use datafusion_common::metadata::FieldMetadata;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{DFSchema, DFSchemaRef, Result, ScalarValue};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::utils::{conjunction, split_conjunction_owned};
use datafusion_expr::{ColumnarValue, Expr, ExprSchemable, LogicalPlan, Operator, lit};

use crate::function::SparkStringTemporalComparisonCast;
use crate::function::common::{
    NormalizeFloatArrayForSparkComparison, SparkComparisonFloat, materialize_metadata_literals,
    restore_inlined_scalar_metadata, set_result_metadata, without_metadata_annotations,
};
use crate::function::float_keys::NormalizeNestedFloatKeys;
use crate::function::result_coercion::{
    has_coalesce_declaration_cast, prune_coalesce_declaration_cast,
};
use crate::function::scalar::SparkArraySlice;
use crate::function::scalar::conditional::SparkNullIf;
use crate::function::scalar::math::{
    FusedDecimalDivide, SparkCheckedDivide, SparkCheckedModulo, SparkIntegerDivide,
    SparkNullIfZero, guard_cast_arithmetic, integer_divide_args,
};
use crate::function::scalar::{lower_runtime_regexp_extract, lower_runtime_substring};
use crate::function::scalar::{
    preserve_local_case_conversion, simplify_projected_case_conversions,
};
use datafusion::functions::math::round::RoundFunc;
use datafusion_expr::simplify::SparkArrayIndexError;
use sail_function::scalar::array::array_contains::ArrayContains;
use sail_function::scalar::array::spark_array::SparkArray;
use sail_function::scalar::datetime::timestamp_now::TimestampNow;
use sail_function::scalar::math::random::Random as SailRandom;
use sail_function::scalar::math::spark_abs::SparkAbs;
use sail_function::scalar::math::spark_checked_arithmetic::SparkCheckedArithmetic;
use sail_function::scalar::math::spark_negative::SparkNegative;
use sail_function::scalar::math::{
    spark_bround::SparkBRound, spark_decimal_round::SparkDecimalRound,
    spark_float_round::SparkFloatRound, spark_integer_round::SparkIntegerRound,
    spark_interval_negate::SparkIntervalNegate, spark_interval_scale::SparkIntervalScale,
};
use sail_function::scalar::misc::raise_error::RaiseError;
use sail_function::scalar::predicate::spark_like::SparkLike;
use sail_function::scalar::spark_cast_numeric_to_integer::SparkCastNumericToInteger;
use sail_function::scalar::spark_cast_string_to_boolean::SparkCastStringToBoolean;
use sail_function::scalar::spark_cast_string_to_decimal::SparkCastStringToDecimal;
use sail_function::scalar::spark_cast_string_to_float::SparkCastStringToFloat;
use sail_function::scalar::spark_cast_string_to_integer::SparkCastStringToInteger;
use sail_function::scalar::spark_decimal_cast::{DecimalCastMode, SparkDecimalCast};
use sail_function::scalar::spark_evaluation::SparkInterpretedEvaluation;
use sail_function::scalar::spark_to_string::{SparkToLargeUtf8, SparkToUtf8, SparkToUtf8View};
use sail_function::scalar::string::spark_regexp_extract_all::SparkRegexpExtract;
use sail_function::scalar::string::spark_regexp_replace::SparkRegexpReplace;
use sail_function::scalar::string::spark_substring::SparkSubstring;
use sail_function::scalar::struct_function::StructFunction;

/// Prepare Spark nested result declarations and early expression errors.
#[derive(Debug)]
pub struct PrepareSparkExpressions;

datafusion_common::extensions_options! {
    /// Query-local Spark declaration and error semantics, absent from native SQL queries.
    pub struct SparkSqlExpressionSemantics {}
}

impl ConfigExtension for SparkSqlExpressionSemantics {
    // Preserve the legacy prefix for configuration and diagnostic compatibility.
    const PREFIX: &'static str = "dar_spark_sql_errors";
}

fn null_literal(expr: &Expr) -> bool {
    let expr = without_metadata_annotations(expr);
    match expr {
        Expr::Literal(value, _) => value.is_null(),
        Expr::Cast(cast) => null_literal(&cast.expr),
        Expr::TryCast(cast) => null_literal(&cast.expr),
        Expr::Alias(alias) => null_literal(&alias.expr),
        Expr::ScalarFunction(f)
            if (f.func.inner().is::<SparkNullIfZero>()
                || f.func.inner().is::<SparkDecimalCast>()
                || f.func.inner().is::<SparkCastNumericToInteger>()
                || f.func.inner().is::<SparkNullIf>()
                || f.func
                    .inner()
                    .is::<datafusion::functions::core::nullif::NullIfFunc>()) =>
        {
            f.args.first().is_some_and(null_literal)
        }
        _ => false,
    }
}

/// Local division and remainder inspect a NULL divisor before their dividend.
pub(crate) fn prune_null_divisor(expr: Expr, schema: &DFSchemaRef) -> Result<Transformed<Expr>> {
    let right = match &expr {
        Expr::BinaryExpr(binary) if matches!(binary.op, Operator::Divide | Operator::Modulo) => {
            Some(binary.right.as_ref())
        }
        Expr::ScalarFunction(f)
            if f.func.inner().is::<SparkCheckedDivide>()
                || f.func.inner().is::<SparkCheckedModulo>()
                || f.func.inner().is::<FusedDecimalDivide>() =>
        {
            Some(&f.args[1])
        }
        _ => integer_divide_args(&expr, schema).map(|(_, right, _)| right),
    };
    if right.is_some_and(null_literal) && !expr.contains_scalar_subquery() {
        Ok(Transformed::yes(lit(ScalarValue::try_new_null(
            &expr.get_type(schema)?,
        )?)))
    } else {
        Ok(Transformed::no(expr))
    }
}

fn projected_null(expr: &Expr, input: &LogicalPlan) -> Result<bool> {
    let expr = without_metadata_annotations(expr);
    match expr {
        Expr::Alias(alias) => projected_null(&alias.expr, input),
        Expr::Cast(cast) => projected_null(&cast.expr, input),
        Expr::TryCast(cast) => projected_null(&cast.expr, input),
        Expr::ScalarFunction(f)
            if f.func.inner().is::<NormalizeFloatArrayForSparkComparison>()
                || (f.func.inner().is::<NormalizeNestedFloatKeys>()
                    && f.func.inner().supports_constant_folding())
                || f.func.inner().is::<SparkDecimalCast>()
                || f.func.inner().is::<SparkCastNumericToInteger>()
                || f.func.inner().is::<SparkComparisonFloat>() =>
        {
            match f.args.as_slice() {
                [arg] => projected_null(arg, input),
                _ => Ok(false),
            }
        }
        Expr::Column(column) => {
            let index = input.schema().index_of_column(column)?;
            match input {
                LogicalPlan::Projection(project) => {
                    projected_null(&project.expr[index], &project.input)
                }
                LogicalPlan::SubqueryAlias(alias) => projected_null(
                    &Expr::Column(alias.input.schema().columns()[index].clone()),
                    &alias.input,
                ),
                _ => Ok(false),
            }
        }
        _ => Ok(null_literal(expr)),
    }
}

fn null_round(expr: &Expr) -> bool {
    let Expr::ScalarFunction(f) = expr else {
        return false;
    };
    let inner = f.func.inner();
    ((inner.is::<SparkDecimalRound>() || inner.is::<RoundFunc>() || inner.is::<SparkBRound>())
        && f.args.get(1).is_some_and(null_literal))
        || inner
            .downcast_ref::<SparkFloatRound>()
            .is_some_and(SparkFloatRound::is_null_scale)
        || inner
            .downcast_ref::<SparkIntegerRound>()
            .is_some_and(SparkIntegerRound::is_null_scale)
}

// Classify the value node itself so rewrites retain any enclosing annotations.
fn null_discarded(expr: &Expr) -> bool {
    match expr {
        Expr::BinaryExpr(binary) => {
            binary.op.returns_null_on_null()
                && (null_literal(&binary.left) || null_literal(&binary.right))
        }
        Expr::ScalarFunction(f) => {
            let inner = f.func.inner();
            ((inner.is::<FusedDecimalDivide>()
                || inner.is::<SparkCheckedDivide>()
                || inner.is::<SparkCheckedModulo>()
                || inner.is::<SparkCheckedArithmetic>()
                || inner.is::<SparkIntervalScale>()
                || inner.is::<SparkIntegerDivide>())
                && f.args.iter().any(null_literal))
                || null_round(expr)
        }
        _ => false,
    }
}

fn candidate(expr: &Expr) -> bool {
    null_discarded(expr) && expr.contains_scalar_subquery()
}

// Check whole parent expressions so CASE and COALESCE retain their own
// evaluation order. Aggregates and sorting expose constant child expressions.
fn arithmetic_candidate(expr: &Expr, schema: &DFSchemaRef) -> Result<bool> {
    expr.exists(|expr| Ok(
        matches!(expr, Expr::BinaryExpr(binary) if matches!(binary.op, Operator::Divide | Operator::Modulo))
        || integer_divide_args(expr, schema).is_some_and(|(_, _, ansi)| ansi)
        || matches!(expr, Expr::ScalarFunction(f)
            if f.func.inner().is::<SparkAbs>()
                || f.func.inner().is::<SparkCheckedArithmetic>()
                || f.func.inner().is::<SparkIntervalScale>()
                || f.func.inner().is::<SparkIntervalNegate>()
                || f.func.inner().is::<SparkCheckedDivide>()
                || f.func.inner().is::<SparkCheckedModulo>()
                || f.func.inner().downcast_ref::<SparkDecimalCast>()
                    .is_some_and(|cast| cast.mode() == DecimalCastMode::Ansi)
                || f.func.inner().is::<FusedDecimalDivide>())
    ))
}

fn array_index_error_candidate(expr: &Expr) -> Result<bool> {
    expr.exists(|expr| {
        Ok(matches!(expr, Expr::ScalarFunction(function)
        if function.func.inner().downcast_ref::<RaiseError>()
            .is_some_and(RaiseError::is_array_index_error)))
    })
}

fn preparation_error_candidate(
    expr: &Expr,
    schema: &DFSchemaRef,
    spark_array_index_errors: bool,
) -> Result<bool> {
    Ok(arithmetic_candidate(expr, schema)?
        || (spark_array_index_errors && array_index_error_candidate(expr)?))
}

pub(crate) fn permits_constant_preparation(expr: &Expr) -> Result<bool> {
    Ok(!expr.exists(|expr| {
        Ok(expr.is_volatile_node()
            || matches!(expr, Expr::ScalarFunction(function)
            if !function.func.inner().supports_constant_folding()))
    })?)
}

fn input_permits_array_index_preparation(plan: &LogicalPlan) -> Result<bool> {
    let mut allowed = true;
    plan.apply(|node| {
        node.apply_expressions(|expr| {
            allowed &= permits_constant_preparation(expr)?;
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(if allowed {
            TreeNodeRecursion::Continue
        } else {
            TreeNodeRecursion::Stop
        })
    })?;
    Ok(allowed)
}

// Follow aliases or already-closed values on a preparation copy; dependent computations stay opaque.
fn inline_closed_projection_bindings_for_array_check(
    expr: Expr,
    input: &LogicalPlan,
) -> Result<Expr> {
    Ok(expr
        .transform_up(|expr| {
            let Expr::Column(column) = &expr else {
                return Ok(Transformed::no(expr));
            };
            let index = input.schema().index_of_column(column)?;
            let source = match input {
                LogicalPlan::Projection(project) => {
                    let source = &project.expr[index];
                    let mut body = source;
                    while let Expr::Alias(alias) = body {
                        body = &alias.expr;
                    }
                    if matches!(body, Expr::Column(_)) {
                        inline_closed_projection_bindings_for_array_check(
                            source.clone(),
                            &project.input,
                        )?
                    } else if is_locally_evaluable(source, false)?
                        && permits_constant_preparation(source)?
                    {
                        source.clone()
                    } else {
                        return Ok(Transformed::no(expr));
                    }
                }
                LogicalPlan::SubqueryAlias(alias) => {
                    inline_closed_projection_bindings_for_array_check(
                        Expr::Column(alias.input.schema().columns()[index].clone()),
                        &alias.input,
                    )?
                }
                _ => return Ok(Transformed::no(expr)),
            };
            if is_locally_evaluable(&source, false)? && permits_constant_preparation(&source)? {
                Ok(Transformed::yes(source))
            } else {
                Ok(Transformed::no(expr))
            }
        })?
        .data)
}

// Spark reduces Boolean literals before pulling correlations, but folds
// comparisons and CAST(NULL AS BOOLEAN) later.
fn simplify_literal_boolean_predicate(predicate: Expr) -> Result<Transformed<Expr>> {
    predicate.transform_up(|expr| {
        use Expr::Literal;
        use Operator::{And, Or};
        use ScalarValue::Boolean;
        let reduced = match &expr {
            Expr::BinaryExpr(binary) => match (&*binary.left, binary.op, &*binary.right) {
                (Literal(Boolean(Some(false)), _), And, _)
                | (_, And, Literal(Boolean(Some(false)), _)) => Some(lit(false)),
                (Literal(Boolean(Some(true)), _), Or, _)
                | (_, Or, Literal(Boolean(Some(true)), _)) => Some(lit(true)),
                (Literal(Boolean(Some(true)), _), And, right)
                | (Literal(Boolean(Some(false)), _), Or, right) => Some(right.clone()),
                (left, And, Literal(Boolean(Some(true)), _))
                | (left, Or, Literal(Boolean(Some(false)), _)) => Some(left.clone()),
                _ => None,
            },
            Expr::Not(inner) => match inner.as_ref() {
                Literal(Boolean(Some(value)), _) => Some(lit(!value)),
                _ => None,
            },
            _ => None,
        };
        Ok(match reduced {
            Some(expr) => Transformed::yes(expr),
            None => Transformed::no(expr),
        })
    })
}

fn simplify_correlated_exists_filters(expr: Expr) -> Result<Transformed<Expr>> {
    let Expr::Exists(mut exists) = expr else {
        return Ok(Transformed::no(expr));
    };
    if exists.subquery.outer_ref_columns.is_empty() {
        return Ok(Transformed::no(Expr::Exists(exists)));
    }
    let simplified = Arc::unwrap_or_clone(exists.subquery.subquery).transform_up(|node| {
        let LogicalPlan::Filter(mut filter) = node else {
            return Ok(Transformed::no(node));
        };
        if !filter.predicate.contains_scalar_subquery() {
            return Ok(Transformed::no(LogicalPlan::Filter(filter)));
        }
        let predicate = simplify_literal_boolean_predicate(filter.predicate)?;
        filter.predicate = predicate.data;
        Ok(Transformed::new(
            LogicalPlan::Filter(filter),
            predicate.transformed,
            TreeNodeRecursion::Continue,
        ))
    })?;
    exists.subquery.subquery = Arc::new(simplified.data);
    Ok(Transformed::new(
        Expr::Exists(exists),
        simplified.transformed,
        TreeNodeRecursion::Continue,
    ))
}

impl AnalyzerRule for PrepareSparkExpressions {
    fn name(&self) -> &str {
        // Historical error normalization depends on this rule label.
        "preserve_spark_expression_errors"
    }

    fn analyze(&self, plan: LogicalPlan, options: &ConfigOptions) -> Result<LogicalPlan> {
        let simplifier = ExprSimplifier::new(
            datafusion_expr::simplify::SimplifyContext::builder()
                .with_config_options(Arc::new(options.clone()))
                .build(),
        );
        // OptimizeRand discards valid seeds before arithmetic and producer preparation.
        let plan = plan
            .transform_up_with_subqueries(|node| {
                let names = datafusion_expr::expr_rewriter::NamePreserver::new(&node);
                let mut fold = |expr: Expr| {
                    let name = names.save(&expr);
                    expr.transform_up(|expr| fold_rand_comparison(expr, &simplifier))
                        .map(|expr| expr.update_data(|expr| name.restore(expr)))
                };
                node.map_expressions(|expr| {
                    if matches!(expr, Expr::GroupingSet(_)) {
                        expr.map_children(&mut fold)
                    } else {
                        fold(expr)
                    }
                })
            })?
            .data;
        let plan = self.prepare(plan, options)?;
        let spark_query = options
            .extensions
            .get::<SparkSqlExpressionSemantics>()
            .is_some();
        Ok(plan
            .transform_up_with_subqueries(|node| {
                let input = if spark_query {
                    match &node {
                        LogicalPlan::Projection(project) => Some(Arc::clone(&project.input)),
                        LogicalPlan::Filter(filter) => Some(Arc::clone(&filter.input)),
                        // Spark projects window arguments; aggregate arguments stay opaque.
                        LogicalPlan::Window(window) => Some(Arc::clone(&window.input)),
                        _ => None,
                    }
                } else {
                    None
                };
                let names = datafusion_expr::expr_rewriter::NamePreserver::new(&node);
                let mut materialize = |expr: Expr| {
                    let name = names.save(&expr);
                    let folded =
                        expr.transform_up(|expr| fold_rand_comparison(expr, &simplifier))?;
                    let canonical = if let Some(input) = &input {
                        folded
                            .data
                            .transform_up(|expr| canonicalize_case_result_columns(expr, input))?
                    } else {
                        Transformed::no(folded.data)
                    };
                    let mut materialized = materialize_metadata_literals(canonical.data)?;
                    materialized.transformed |= folded.transformed || canonical.transformed;
                    Ok(materialized.update_data(|expr| name.restore(expr)))
                };
                node.map_expressions(|expr| {
                    if matches!(expr, Expr::GroupingSet(_)) {
                        expr.map_children(&mut materialize)
                    } else {
                        materialize(expr)
                    }
                })
            })?
            .data)
    }
}

fn spark_rand(expr: &Expr) -> Option<&ScalarFunction> {
    match without_metadata_annotations(expr) {
        Expr::ScalarFunction(function) if function.func.inner().is::<SailRandom>() => {
            Some(function)
        }
        Expr::ScalarFunction(function)
            if function.func.inner().is::<SparkComparisonFloat>()
                && let [argument] = function.args.as_slice() =>
        {
            spark_rand(argument)
        }
        Expr::Alias(alias) => spark_rand(&alias.expr),
        _ => None,
    }
}

fn fold_rand_comparison(expr: Expr, simplifier: &ExprSimplifier) -> Result<Transformed<Expr>> {
    let Expr::BinaryExpr(binary) = &expr else {
        return Ok(Transformed::no(expr));
    };
    if !matches!(
        binary.op,
        Operator::Gt | Operator::GtEq | Operator::Lt | Operator::LtEq
    ) {
        return Ok(Transformed::no(expr));
    }
    let (random, bound, op) = if let Some(random) = spark_rand(&binary.left) {
        (random, binary.right.as_ref(), binary.op)
    } else if let Some(random) = spark_rand(&binary.right) {
        let Some(op) = binary.op.swap() else {
            return Ok(Transformed::no(expr));
        };
        (random, binary.left.as_ref(), op)
    } else {
        return Ok(Transformed::no(expr));
    };
    match random.args.as_slice() {
        [] => {}
        // Admission is structural: a discarded seed need not evaluate successfully.
        [seed]
            if is_locally_evaluable(seed, false)?
                && matches!(
                    seed.get_type(&DFSchema::empty())?,
                    DataType::Int64 | DataType::UInt64 | DataType::Null
                ) => {}
        _ => return Ok(Transformed::no(expr)),
    }
    if !is_locally_evaluable(bound, false)? {
        return Ok(Transformed::no(expr));
    }
    let Ok(Expr::Literal(ScalarValue::Float64(Some(value)), _)) =
        simplifier.simplify(bound.clone())
    else {
        return Ok(Transformed::no(expr));
    };
    // Spark RAND is in [0, 1); interior comparisons must remain volatile.
    let result = match op {
        Operator::Gt if value < 0.0 => true,
        Operator::Gt | Operator::GtEq if value >= 1.0 => false,
        Operator::GtEq if value <= 0.0 => true,
        Operator::Lt | Operator::LtEq if value >= 1.0 => true,
        Operator::Lt if value <= 0.0 => false,
        Operator::LtEq if value < 0.0 => false,
        _ => return Ok(Transformed::no(expr)),
    };
    Ok(Transformed::yes(lit(result)))
}

fn passthrough_column_origin<'a>(
    input: &'a LogicalPlan,
    column: &datafusion_common::Column,
) -> Result<(&'a LogicalPlan, usize)> {
    let index = input.schema().index_of_column(column)?;
    match input {
        LogicalPlan::SubqueryAlias(alias) => {
            let column = alias.input.schema().qualified_field(index).into();
            passthrough_column_origin(&alias.input, &column)
        }
        LogicalPlan::Filter(filter) => {
            // Row selection preserves the existing column values of each surviving row.
            let column = filter.input.schema().qualified_field(index).into();
            passthrough_column_origin(&filter.input, &column)
        }
        LogicalPlan::Projection(project) => {
            let mut expression = &project.expr[index];
            while let Expr::Alias(alias) = expression {
                if alias
                    .metadata
                    .as_ref()
                    .is_some_and(|metadata| !metadata.is_empty())
                {
                    return Ok((input, index));
                }
                expression = &alias.expr;
            }
            if let Expr::Column(column) = expression {
                return passthrough_column_origin(&project.input, column);
            }
            // A computed output is an existing value, never an expression to inline.
            Ok((input, index))
        }
        _ => Ok((input, index)),
    }
}

// Normalize stored result references; the shared CASE rule still owns guard pruning.
fn canonicalize_case_result_columns(expr: Expr, input: &LogicalPlan) -> Result<Transformed<Expr>> {
    let Expr::Case(mut case) = expr else {
        return Ok(Transformed::no(expr));
    };
    if !case.is_spark_searched() {
        return Ok(Transformed::no(Expr::Case(case)));
    }
    let Some(Expr::Column(fallback)) = case.else_expr.as_deref() else {
        return Ok(Transformed::no(Expr::Case(case)));
    };
    let fallback = fallback.clone();
    let fallback_field = input
        .schema()
        .field(input.schema().index_of_column(&fallback)?);
    let (fallback_plan, fallback_index) = passthrough_column_origin(input, &fallback)?;
    let mut changed = false;
    for (_, result) in &case.when_then_expr {
        let Expr::Column(column) = result.as_ref() else {
            return Ok(Transformed::no(Expr::Case(case)));
        };
        let field = input
            .schema()
            .field(input.schema().index_of_column(column)?);
        if field
            .as_ref()
            .clone()
            .with_name(fallback_field.name().as_str())
            != *fallback_field.as_ref()
        {
            return Ok(Transformed::no(Expr::Case(case)));
        }
        let (origin_plan, origin_index) = passthrough_column_origin(input, column)?;
        // Separate outputs can have equal expressions or rows without being the same stored value.
        if !std::ptr::eq(origin_plan, fallback_plan) || origin_index != fallback_index {
            return Ok(Transformed::no(Expr::Case(case)));
        }
        changed |= column != &fallback;
    }
    if changed {
        for (_, result) in &mut case.when_then_expr {
            **result = Expr::Column(fallback.clone());
        }
    }
    Ok(Transformed::new_transformed(Expr::Case(case), changed))
}

#[test]
fn case_result_columns_require_the_same_existing_value_and_fields() -> Result<()> {
    use datafusion_expr::{Case, LogicalPlanBuilder, col};

    let values = LogicalPlanBuilder::values(vec![vec![lit(7_i32), lit(7_i32)]])?.build()?;
    let shared = LogicalPlanBuilder::from(values.clone())
        .project(vec![col("column1").alias("a"), col("column1").alias("b")])?
        .alias("q")?
        .build()?;
    let filtered = LogicalPlanBuilder::from(shared.clone())
        .filter(col("q.a").is_not_null())?
        .build()?;
    let distinct = LogicalPlanBuilder::from(values.clone())
        .project(vec![col("column1").alias("a"), col("column2").alias("b")])?
        .alias("q")?
        .build()?;
    let annotated = |name: &str| {
        col("column1").alias_with_metadata(
            name,
            Some(FieldMetadata::from(std::collections::HashMap::from([(
                "source".to_owned(),
                name.to_owned(),
            )]))),
        )
    };
    let metadata = LogicalPlanBuilder::from(values)
        .project(vec![annotated("a"), annotated("b")])?
        .alias("q")?
        .build()?;
    let volatile = LogicalPlanBuilder::empty(true)
        .project(vec![expr_fn::uuid().alias("a"), expr_fn::uuid().alias("b")])?
        .alias("q")?
        .build()?;
    let materialized = LogicalPlanBuilder::empty(true)
        .project(vec![expr_fn::uuid().alias("u")])?
        .project(vec![col("u").alias("a"), col("u").alias("b")])?
        .alias("q")?
        .build()?;
    let guard = col("q.a").is_not_null();
    let expression = Expr::Case(Case::new_spark_searched(
        vec![(Box::new(guard.clone()), Box::new(col("q.a")))],
        Some(Box::new(col("q.b"))),
    ));
    let canonical = Expr::Case(Case::new_spark_searched(
        vec![(Box::new(guard.clone()), Box::new(col("q.b")))],
        Some(Box::new(col("q.b"))),
    ));
    for (input, changed) in [
        (shared.clone(), true),
        (filtered, true),
        (distinct, false),
        (metadata, false),
        (volatile, false),
        (materialized, true),
    ] {
        let actual = canonicalize_case_result_columns(expression.clone(), &input)?;
        assert_eq!(actual.transformed, changed);
        assert_eq!(
            actual.data,
            if changed {
                canonical.clone()
            } else {
                expression.clone()
            }
        );
    }
    let native = Expr::Case(Case::new(
        None,
        vec![(Box::new(guard), Box::new(col("q.a")))],
        Some(Box::new(col("q.b"))),
    ));
    assert_eq!(
        canonicalize_case_result_columns(native.clone(), &shared)?.data,
        native
    );
    Ok(())
}

impl PrepareSparkExpressions {
    fn prepare(&self, plan: LogicalPlan, options: &ConfigOptions) -> Result<LogicalPlan> {
        // Borrow first: ordinary queries require no rewritten plan or batch allocation.
        let is_spark_query = options
            .extensions
            .get::<SparkSqlExpressionSemantics>()
            .is_some();
        let plan = if is_spark_query {
            preserve_local_case_conversions(preserve_local_string_arguments(plan)?)?
        } else {
            plan
        };
        let mut found = false;
        let mut check_preparation_errors = false;
        let mut needs_reachability_check = false;
        let mut has_correlated_scalar = false;
        let mut check_groups = false;
        let mut has_null_sensitive_comparison = false;
        let mut has_groups = false;
        let mut has_constant_struct = false;
        let mut has_coalesce_declaration_casts = false;
        let mut has_slice_parameters = false;
        plan.apply_with_subqueries(|node| {
            if let LogicalPlan::Aggregate(aggregate) = node
                && !aggregate
                    .group_expr
                    .iter()
                    .any(|expr| matches!(expr, Expr::GroupingSet(_)))
            {
                has_groups |= !aggregate.group_expr.is_empty();
                for expr in &aggregate.group_expr {
                    check_groups |= is_locally_evaluable(expr, false)?;
                }
            }
            node.apply_expressions(|expr| {
                found |= expr.exists(|expr| Ok(candidate(expr)))?;
                has_constant_struct |= is_spark_query && expr.exists(|expr| {
                    Ok(matches!(expr, Expr::ScalarFunction(f) if f.func.inner().is::<StructFunction>())
                        && is_locally_evaluable(expr, false)?)
                })?;
                has_coalesce_declaration_casts |= is_spark_query && expr.exists(|expr| {
                    Ok(has_coalesce_declaration_cast(expr))
                })?;
                // Strict constant UDFs need projection pruning before ordinary folding.
                needs_reachability_check |= has_cast(expr)?
                    || (is_spark_query && expr.exists(|expr| {
                        Ok(matches!(expr, Expr::ScalarFunction(function)
                            if function.func.inner().propagates_constant_folding_errors()))
                    })?);
                has_correlated_scalar |= is_spark_query && expr.exists(|expr| {
                    Ok(matches!(expr, Expr::ScalarSubquery(subquery) if !subquery.outer_ref_columns.is_empty()))
                })?;
                check_preparation_errors |= preparation_error_candidate(expr, node.schema(), is_spark_query)?;
                has_slice_parameters |= expr.exists(|expr| {
                    Ok(matches!(expr, Expr::ScalarFunction(f) if f.func.inner().is::<SparkArraySlice>()))
                })?;
                has_null_sensitive_comparison |= expr.exists(|expr| {
                    Ok(matches!(expr, Expr::ScalarFunction(f) if f.func.inner().is::<ArrayContains>() || f.func.inner().is::<SparkNullIf>()))
                })?;
                Ok(TreeNodeRecursion::Continue)
            })?;
            Ok(if found && check_preparation_errors && needs_reachability_check && check_groups && has_null_sensitive_comparison && has_constant_struct && has_coalesce_declaration_casts && has_slice_parameters {
                TreeNodeRecursion::Stop
            } else {
                TreeNodeRecursion::Continue
            })
        })?;
        if !found
            && !check_preparation_errors
            && !needs_reachability_check
            && !has_correlated_scalar
            && !check_groups
            && !has_null_sensitive_comparison
            && !has_constant_struct
            && !has_coalesce_declaration_casts
            && !has_slice_parameters
        {
            return Ok(plan);
        }
        // These checks reproduce Spark's interpreted evaluation of constants and local rows.
        let mut evaluation_options = options.clone();
        evaluation_options
            .extensions
            .insert(SparkInterpretedEvaluation::default());
        let mut props = ExecutionProps::new();
        props.config_options = Some(Arc::new(evaluation_options));
        props.query_execution_start_time = crate::like_errors::spark_query_start_time(options);
        let prepare = Prepare {
            props,
            has_groups,
            has_null_sensitive_comparison,
            spark_array_index_errors: is_spark_query,
        };
        let plan = if has_constant_struct || has_coalesce_declaration_casts {
            prepare.prepare_nested_result_declarations(plan)?
        } else {
            plan
        };
        let plan = if check_groups && is_spark_query {
            plan.transform_down_with_subqueries(prepare_literal_group_consumers)?
                .data
                .transform_up_with_subqueries(|plan| prepare_foldable_groups(plan, options))?
                .data
        } else {
            plan
        };
        let plan = if needs_reachability_check || check_preparation_errors {
            plan.transform_down_up_with_subqueries(|node| {
                // Correlated EXISTS drops unused filter subqueries before their preparation.
                let simplified = if is_spark_query {
                    node.map_expressions(|expr| {
                        expr.transform_down(simplify_correlated_exists_filters)
                    })?
                } else {
                    Transformed::no(node)
                };
                let changed = simplified.transformed;
                let node = simplified.data;
                // Local empty conversion precedes preparing scalar children.
                // Keep the existing postorder work for all remaining nodes.
                let scalar_children = match &node {
                    LogicalPlan::Projection(project) => project.expr.iter().any(Expr::contains_scalar_subquery),
                    LogicalPlan::Filter(filter) => filter.predicate.contains_scalar_subquery(),
                    LogicalPlan::Aggregate(aggregate) => aggregate.group_expr.iter()
                        .chain(&aggregate.aggr_expr).any(Expr::contains_scalar_subquery),
                    _ => false,
                };
                if scalar_children
                    && prepare.local_batch(&node)?.is_some_and(|batch| batch.num_rows() == 0)
                {
                    if !matches!(node, LogicalPlan::Projection(_)) {
                        // Keep aggregate/filter field nullability. Their parent
                        // output projection still supplies IN/scalar expressions.
                        return Ok(Transformed::yes(LogicalPlan::EmptyRelation(
                            datafusion_expr::logical_plan::EmptyRelation {
                                produce_one_row: false,
                                schema: Arc::clone(node.schema()),
                            },
                        )));
                    }
                    // IN/scalar lowering still needs a named output expression.
                    let schema = node.schema();
                    let expr = schema.iter().map(|(qualifier, field)| {
                        Ok(lit(ScalarValue::try_new_null(field.data_type())?)
                            .alias_qualified(qualifier.cloned(), field.name()))
                    }).collect::<Result<Vec<_>>>()?;
                    let input = LogicalPlan::EmptyRelation(
                        datafusion_expr::logical_plan::EmptyRelation {
                            produce_one_row: false,
                            schema: Arc::clone(node.inputs()[0].schema()),
                        },
                    );
                    return Ok(Transformed::yes(LogicalPlan::Projection(
                        datafusion_expr::logical_plan::Projection::try_new_with_schema(
                            expr, Arc::new(input), Arc::clone(schema),
                        )?,
                    )));
                }
                Ok(Transformed::new(node, changed, TreeNodeRecursion::Continue))
            }, |node| {
                let schema = if !node.inputs().is_empty() {
                    Arc::new(datafusion_expr::utils::merge_schema(&node.inputs()))
                } else if let LogicalPlan::TableScan(scan) = &node {
                    Arc::new(DFSchema::try_from_qualified_schema(
                        scan.table_name.clone(),
                        &scan.source.schema(),
                    )?)
                } else {
                    Arc::new(DFSchema::empty())
                };
                let names = datafusion_expr::expr_rewriter::NamePreserver::new(&node);
                node.map_expressions(|expr| {
                    let name = names.save(&expr);
                    Ok(expr
                        .transform_up(|mut expr| {
                            // EXISTS needs only row presence. Keep local VALUES
                            // checks before pruning its unused output expressions.
                            let mut prune_exists_output = false;
                            let predicate = match &mut expr {
                                Expr::Exists(exists) if is_spark_query => {
                                    // Losing the correlation stops output pruning,
                                    // not local-input or arithmetic preparation.
                                    prune_exists_output = exists.subquery.outer_ref_columns.is_empty()
                                        || prepare.correlated_exists_uses_inner_columns(&exists.subquery.subquery)?;
                                    // Remaining scalar subqueries are prepared even
                                    // when EXISTS will discard their parent projection.
                                    exists.subquery.subquery.apply_with_subqueries(|node| {
                                        node.apply_expressions(|expr| {
                                            expr.apply(|expr| {
                                                if let Expr::ScalarSubquery(scalar) = expr {
                                                    prepare.check_subquery(&scalar.subquery)?;
                                                }
                                                Ok(TreeNodeRecursion::Continue)
                                            })
                                        })
                                    })?;
                                    Some(&mut exists.subquery)
                                }
                                Expr::InSubquery(subquery)
                                    if subquery.subquery.outer_ref_columns.is_empty() =>
                                {
                                    Some(&mut subquery.subquery)
                                }
                                _ => None,
                            };
                            if let Some(subquery) = predicate {
                                if check_preparation_errors {
                                    prepare.check_preparation_error_expressions(&subquery.subquery, true)?;
                                }
                                let prepared = prepare.cast_reachability(
                                    Arc::unwrap_or_clone(Arc::clone(&subquery.subquery)),
                                    options,
                                    prune_exists_output,
                                )?;
                                // LIMIT 0 may sit below a projection or alias.
                                // Reuse native empty propagation before constant checks.
                                let config = OptimizerContext::new_with_config_options(Arc::new(options.clone()));
                                let prepared = prepared.transform_up(|node| {
                                    datafusion::optimizer::propagate_empty_relation::PropagateEmptyRelation::new()
                                        .rewrite(node, &config)
                                })?.data;
                                if check_preparation_errors {
                                    prepare.check_preparation_error_plan(&prepared, options, true)?;
                                }
                                subquery.subquery = Arc::new(prepared);
                                if let Expr::InSubquery(in_subquery) = &mut expr
                                    && in_subquery.subquery.subquery.head_output_expr()?.is_none()
                                    && let LogicalPlan::EmptyRelation(empty) = in_subquery.subquery.subquery.as_ref()
                                    && !empty.produce_one_row
                                {
                                    // Predicate lowering still requires a head expression.
                                    // A typed NULL projection over zero rows preserves it.
                                    let field = empty.schema.field(0);
                                    in_subquery.subquery.subquery = Arc::new(LogicalPlan::Projection(
                                        datafusion_expr::logical_plan::Projection::try_new(
                                            vec![lit(ScalarValue::try_new_null(field.data_type())?).alias(field.name())],
                                            Arc::clone(&in_subquery.subquery.subquery),
                                        )?,
                                    ));
                                }
                                return Ok(Transformed::yes(expr));
                            }
                            // One-row scalar expressions follow their parent's
                            // reachability before standalone subquery folding.
                            if needs_reachability_check && let Expr::ScalarSubquery(mut subquery) = expr {
                                if let Some(mut expressions) = one_row_exprs(&subquery.subquery)?
                                    && expressions.len() == 1
                                    && is_locally_evaluable(&expressions[0], true)?
                                {
                                    return Ok(Transformed::yes(expressions.remove(0)));
                                }
                                // Preserve local arithmetic errors before CAST
                                // preparation removes unused or empty projections.
                                if check_preparation_errors {
                                    prepare.check_preparation_error_plan(&subquery.subquery, options, false)?;
                                }
                                subquery.subquery = Arc::new(prepare.cast_reachability(
                                    Arc::unwrap_or_clone(subquery.subquery),
                                    options,
                                    false,
                                )?);
                                return Ok(Transformed::yes(Expr::ScalarSubquery(subquery)));
                            }
                            if needs_reachability_check {
                                let restored = restore_inlined_scalar_metadata(expr);
                                if restored.transformed {
                                    return Ok(restored);
                                }
                                guard_cast_arithmetic(restored.data, &schema)
                            } else {
                                Ok(Transformed::no(expr))
                            }
                        })?
                        .update_data(|expr| name.restore(expr)))
                })
            })?
            .data
        } else {
            plan
        };
        // The same OneRowRelation rewrite must work without an incidental CAST.
        let plan = if has_correlated_scalar && !needs_reachability_check {
            plan.transform_up_with_subqueries(|node| {
                let schema = datafusion_expr::utils::merge_schema(&node.inputs());
                let names = datafusion_expr::expr_rewriter::NamePreserver::new(&node);
                node.map_expressions(|expr| {
                    let name = names.save(&expr);
                    Ok(expr.transform_up(|expr| {
                        if let Expr::ScalarSubquery(subquery) = &expr
                            && !subquery.outer_ref_columns.is_empty()
                            && subquery.outer_ref_columns.iter().all(|outer| {
                                matches!(outer, Expr::OuterReferenceColumn(_, column) if schema.has_column(column))
                            })
                            && let Some(mut expressions) = one_row_exprs(&subquery.subquery)?
                            && expressions.len() == 1
                            && is_locally_evaluable(&expressions[0], true)?
                        {
                            return Ok(Transformed::yes(expressions.remove(0)));
                        }
                        Ok(restore_inlined_scalar_metadata(expr))
                    })?.update_data(|expr| name.restore(expr)))
                })
            })?.data
        } else {
            plan
        };
        if check_preparation_errors {
            // Reachability preparation already checked these scalar plans before pruning.
            if !needs_reachability_check {
                // Spark prepares subqueries before pruning their outer expressions.
                // Keep each plan's own local conversion and LIMIT 0 boundary.
                plan.apply_with_subqueries(|node| {
                    node.apply_expressions(|expr| {
                        expr.apply(|expr| {
                            if let Expr::ScalarSubquery(subquery) = expr
                                // OneRowRelation is inlined before subquery preparation;
                                // its arithmetic still follows the outer NULL/CASE rules.
                                && one_row_exprs(&subquery.subquery)?.is_none()
                            {
                                prepare.check_preparation_error_plan(
                                    &subquery.subquery,
                                    options,
                                    false,
                                )?;
                            }
                            Ok(TreeNodeRecursion::Continue)
                        })
                    })
                })?;
            }
            prepare.check_preparation_error_plan(&plan, options, false)?;
        }

        let plan = if needs_reachability_check {
            prepare.cast_reachability(plan, options, false)?
        } else if check_preparation_errors {
            let prepared = prepare.prepare_distinct_limits(plan, 0)?;
            if prepared.transformed || has_groups || has_null_sensitive_comparison {
                let config = OptimizerContext::new_with_config_options(Arc::new(options.clone()));
                prepare.prune_projections(prepared.data, &config, false)?
            } else {
                prepared.data
            }
        } else if has_null_sensitive_comparison {
            let config = OptimizerContext::new_with_config_options(Arc::new(options.clone()));
            prepare.prune_projections(plan, &config, false)?
        } else {
            plan
        };
        if has_slice_parameters {
            prepare.check_constant_slice_parameters(&plan)?;
        }
        if !found {
            return Ok(plan);
        }
        Ok(plan
            .transform_down_with_subqueries(|node| {
                let schema = Arc::new(datafusion_expr::utils::merge_schema(&node.inputs()));
                let names = datafusion_expr::expr_rewriter::NamePreserver::new(&node);
                node.map_expressions(|expr| {
                    let name = names.save(&expr);
                    let rewritten = expr.transform_down(|expr| {
                        // Inlining can remove the last scalar subquery while
                        // its NULL parent still needs the same propagation rule.
                        if !null_discarded(&expr) {
                            return Ok(Transformed::no(expr));
                        }
                        prepare.check_expr(&expr, false)?;
                        let data_type = expr.get_type(&schema)?;
                        if null_round(&expr) {
                            let mut subqueries = Vec::new();
                            round_subqueries(&expr, &mut subqueries, &prepare, &schema)?;
                            if !subqueries.is_empty() {
                                return Ok(Transformed::yes(
                                    datafusion_expr::ScalarUDF::from(NullAfterSubqueries {
                                        signature: datafusion_expr::Signature::variadic_any(
                                            datafusion_expr::Volatility::Immutable,
                                        ),
                                        data_type,
                                    })
                                    .call(subqueries),
                                ));
                            }
                        }
                        Ok(Transformed::yes(lit(ScalarValue::try_new_null(
                            &data_type,
                        )?)))
                    })?;
                    Ok(rewritten.update_data(|expr| name.restore(expr)))
                })
            })?
            .data)
    }
}

// ROUND's NULL scale skips its value expression, but Spark still executes any
// remaining scalar subqueries. Keep only those arguments for the native scalar
// subquery executor; one-row subqueries have already become scalar expressions.
fn round_subqueries(
    expr: &Expr,
    out: &mut Vec<Expr>,
    prepare: &Prepare,
    schema: &DFSchemaRef,
) -> Result<()> {
    let expr = without_metadata_annotations(expr);
    if null_discarded(expr) && !null_round(expr) {
        return Ok(());
    }
    if let Expr::ScalarSubquery(subquery) = expr {
        if let Some(expressions) = one_row_exprs(&subquery.subquery)? {
            return round_subqueries(&expressions[0], out, prepare, schema);
        }
        if !prepare
            .local_batch(&subquery.subquery)?
            .is_some_and(|batch| batch.num_rows() == 0)
        {
            // Empty local input has already discarded its projection. Do not
            // expose those dead expressions to another constant-folding pass.
            out.push(expr.clone());
        }
        return Ok(());
    }
    if matches!(expr, Expr::Case(_))
        || matches!(expr, Expr::ScalarFunction(f) if f.func.short_circuits())
    {
        let info = datafusion_expr::simplify::SimplifyContext::builder()
            .with_schema(Arc::clone(schema))
            .with_config_options(Arc::clone(prepare.props.config_options.as_ref().unwrap()))
            .build();
        let simplifier = ExprSimplifier::new(info);
        let mut conditional = expr.clone();
        if let Expr::Case(case) = &mut conditional {
            for condition in case
                .expr
                .iter_mut()
                .chain(case.when_then_expr.iter_mut().map(|(when, _)| when))
            {
                if let Ok(folded) = simplifier.simplify(*condition.clone()) {
                    **condition = folded;
                }
            }
        } else if let Expr::ScalarFunction(function) = &mut conditional
            && function.func.inner().returns_first_non_null_argument()
        {
            // A nullable cast can become a known value after constant folding.
            for argument in &mut function.args {
                if let Ok(folded) = simplifier.simplify(argument.clone()) {
                    *argument = folded;
                }
            }
        }
        // Failed constant folds stay opaque under ROUND's NULL scale; only
        // subqueries surviving conditional pruning still require execution.
        if let Ok(simplified) = simplifier.simplify_algebraic_once(conditional)
            && simplified.data != *expr
        {
            return round_subqueries(&simplified.data, out, prepare, schema);
        }
    }
    expr.apply_children(|child| {
        round_subqueries(child, out, prepare, schema)?;
        Ok(TreeNodeRecursion::Continue)
    })
    .map(|_| ())
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct NullAfterSubqueries {
    signature: datafusion_expr::Signature,
    data_type: DataType,
}

impl datafusion_expr::ScalarUDFImpl for NullAfterSubqueries {
    fn name(&self) -> &str {
        "spark_null_after_subqueries"
    }
    fn signature(&self) -> &datafusion_expr::Signature {
        &self.signature
    }
    fn return_type(&self, _types: &[DataType]) -> Result<DataType> {
        Ok(self.data_type.clone())
    }
    fn invoke_with_args(
        &self,
        _args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::try_new_null(
            &self.data_type,
        )?))
    }
}

fn literal_group_constructor(expr: &Expr) -> bool {
    match without_metadata_annotations(expr) {
        Expr::Alias(alias) => literal_group_constructor(&alias.expr),
        Expr::Literal(_, _) => true,
        Expr::Cast(cast) => literal_group_constructor(&cast.expr),
        Expr::TryCast(cast) => literal_group_constructor(&cast.expr),
        Expr::Negative(value) => literal_group_constructor(value),
        Expr::ScalarFunction(function)
            if function.func.signature().volatility == datafusion_expr::Volatility::Immutable
                && function.func.inner().supports_constant_folding()
                && (function.func.inner().is::<SparkArray>()
                    || function.func.inner().is::<SparkCastStringToFloat>()
                    || function.func.inner().is::<SparkCastStringToInteger>()
                    || function.func.inner().is::<SparkCastStringToDecimal>()
                    || function.func.inner().is::<SparkDecimalCast>()) =>
        {
            function.args.iter().all(literal_group_constructor)
        }
        _ => false,
    }
}

fn projected_literal_group_constructor(plan: &LogicalPlan, index: usize) -> Result<Option<Expr>> {
    let value = match plan {
        LogicalPlan::Projection(projection) => {
            let Some(mut expression) = projection.expr.get(index) else {
                return Ok(None);
            };
            while let Expr::Alias(alias) = expression {
                expression = &alias.expr;
            }
            let Expr::Column(column) = expression else {
                return Ok(None);
            };
            let Some(index) = projection.input.schema().maybe_index_of_column(column) else {
                return Ok(None);
            };
            projected_literal_group_constructor(&projection.input, index)?
        }
        LogicalPlan::SubqueryAlias(alias) => {
            projected_literal_group_constructor(&alias.input, index)?
        }
        LogicalPlan::Aggregate(aggregate)
            if !aggregate
                .group_expr
                .iter()
                .any(|expr| matches!(expr, Expr::GroupingSet(_))) =>
        {
            datafusion_expr::utils::grouping_set_to_exprlist(&aggregate.group_expr)?
                .into_iter()
                .nth(index)
                .filter(|expr| literal_group_constructor(expr))
                .cloned()
        }
        _ => None,
    };
    let Some(value) = value else {
        return Ok(None);
    };
    let Some(field) = plan.schema().fields().get(index) else {
        return Ok(None);
    };
    if value.get_type(plan.schema())? != *field.data_type() {
        return Ok(None);
    }
    Ok(Some(value))
}

// Bind consumers before native folding erases computed SELECT producers' provenance.
fn prepare_literal_group_consumers(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let LogicalPlan::Projection(projection) = &plan else {
        return Ok(Transformed::no(plan));
    };
    let input = Arc::clone(&projection.input);
    let names = datafusion_expr::expr_rewriter::NamePreserver::new(&plan);
    plan.map_expressions(|expr| {
        let mut body = &expr;
        while let Expr::Alias(alias) = body {
            body = &alias.expr;
        }
        if matches!(body, Expr::Column(_)) {
            return Ok(Transformed::no(expr));
        }
        let name = names.save(&expr);
        expr.transform_down(|expr| {
            if let Expr::Column(column) = &expr
                && let Some(index) = input.schema().maybe_index_of_column(column)
                && let Some(value) = projected_literal_group_constructor(&input, index)?
            {
                let metadata = input.schema().field(index).metadata().clone();
                return Ok(Transformed::new(
                    set_result_metadata(value.unalias(), metadata.into_iter().collect()),
                    true,
                    TreeNodeRecursion::Jump,
                ));
            }
            Ok(Transformed::no(expr))
        })
        .map(|expr| expr.update_data(|expr| name.restore(expr)))
    })
}

// Keep foldable outputs available for live uses, but do not evaluate them
// merely to form groups. A real key column preserves zero groups on empty input.
fn prepare_foldable_groups(
    plan: LogicalPlan,
    options: &ConfigOptions,
) -> Result<Transformed<LogicalPlan>> {
    use datafusion_expr::{Aggregate, Projection};
    if options
        .extensions
        .get::<SparkSqlExpressionSemantics>()
        .is_none()
    {
        return Ok(Transformed::no(plan));
    }
    let LogicalPlan::Aggregate(aggregate) = plan else {
        return Ok(Transformed::no(plan));
    };
    if aggregate
        .group_expr
        .iter()
        .any(|expr| matches!(expr, Expr::GroupingSet(_)))
    {
        return Ok(Transformed::no(LogicalPlan::Aggregate(aggregate)));
    }
    let foldable = aggregate
        .group_expr
        .iter()
        .map(|expr| is_locally_evaluable(expr, false))
        .collect::<Result<Vec<_>>>()?;
    if !foldable.contains(&true) {
        return Ok(Transformed::no(LogicalPlan::Aggregate(aggregate)));
    }
    let mut required = aggregate
        .group_expr
        .iter()
        .zip(&foldable)
        .filter(|(_, constant)| !**constant)
        .map(|(expr, _)| expr.clone())
        .collect::<Vec<_>>();
    let mut input = aggregate.input;
    if required.is_empty() {
        // A literal directly in Aggregate would be removed by DataFusion's
        // EliminateGroupByConstant, changing empty grouped input into one row.
        let mut name = "__spark_grouping_key".to_owned();
        while input
            .schema()
            .fields()
            .iter()
            .any(|field| field.name() == &name)
        {
            name.push('_');
        }
        let mut expr = input
            .schema()
            .columns()
            .into_iter()
            .map(Expr::Column)
            .collect::<Vec<_>>();
        expr.push(lit(0_i32).alias(&name));
        input = Arc::new(LogicalPlan::Projection(Projection::try_new(expr, input)?));
        required.push(Expr::Column(datafusion_common::Column::new_unqualified(
            name,
        )));
    }
    let mut output = Vec::with_capacity(aggregate.schema.fields().len());
    for (i, column) in aggregate.schema.columns().into_iter().enumerate() {
        if i < foldable.len() && foldable[i] {
            output.push(
                aggregate.group_expr[i]
                    .clone()
                    .unalias()
                    .alias_qualified(column.relation, column.name),
            );
        } else {
            output.push(Expr::Column(column));
        }
    }
    let input = LogicalPlan::Aggregate(Aggregate::try_new(input, required, aggregate.aggr_expr)?);
    Ok(Transformed::yes(LogicalPlan::Projection(
        Projection::try_new_with_schema(output, Arc::new(input), aggregate.schema)?,
    )))
}

struct Prepare {
    props: ExecutionProps,
    has_groups: bool,
    has_null_sensitive_comparison: bool,
    spark_array_index_errors: bool,
}

// Preserve the early local row-evaluation boundary before logical folding.
// The classification never evaluates rows or callbacks.
fn preserve_local_string_arguments(plan: LogicalPlan) -> Result<LogicalPlan> {
    fn expressions(exprs: &[Expr], columns: bool) -> Result<bool> {
        exprs.iter().try_fold(true, |local, expr| {
            Ok(local && is_locally_evaluable_with_volatility(expr, columns, true)?)
        })
    }
    fn local_input(plan: &LogicalPlan) -> Result<bool> {
        match plan {
            LogicalPlan::Values(values) => values
                .values
                .iter()
                .try_fold(true, |local, row| Ok(local && expressions(row, false)?)),
            LogicalPlan::EmptyRelation(_) => Ok(true),
            LogicalPlan::Projection(project) => {
                Ok(local_input(&project.input)? && expressions(&project.expr, true)?)
            }
            LogicalPlan::SubqueryAlias(alias) => local_input(&alias.input),
            LogicalPlan::Filter(filter) => Ok(local_input(&filter.input)?
                && is_locally_evaluable_with_volatility(&filter.predicate, true, true)?),
            LogicalPlan::Limit(limit) => {
                let literal_count = |expr: &Expr| match expr {
                    Expr::Literal(ScalarValue::Int32(Some(n)), _) => *n >= 0,
                    Expr::Literal(ScalarValue::Int64(Some(n)), _) => *n >= 0,
                    Expr::Cast(cast) if cast.field.data_type() == &DataType::Int64 => {
                        matches!(cast.expr.as_ref(), Expr::Literal(ScalarValue::Int32(Some(n)), _) if *n >= 0)
                    }
                    _ => false,
                };
                Ok(limit.skip.is_none()
                    && limit.fetch.as_deref().is_some_and(literal_count)
                    && local_input(&limit.input)?)
            }
            _ => Ok(false),
        }
    }
    Ok(plan
        .transform_up_with_subqueries(|node| {
            let input = match &node {
                LogicalPlan::Projection(project) => Some(&project.input),
                LogicalPlan::Filter(filter) => Some(&filter.input),
                _ => None,
            };
            let Some(input) = input else {
                return Ok(Transformed::no(node));
            };
            if !local_input(input)? {
                return Ok(Transformed::no(node));
            }
            let local_values = input.exists(|node| Ok(matches!(node, LogicalPlan::Values(_))))?;
            let names = datafusion_expr::expr_rewriter::NamePreserver::new(&node);
            node.map_expressions(|expression| {
                let name = names.save(&expression);
                expression
                    .transform_up(|expression| {
                        lower_runtime_substring(expression, false)
                            .and_then(|substring| {
                                lower_runtime_regexp_extract(substring.data, local_values).map(
                                    |extract| {
                                        Transformed::new_transformed(
                                            extract.data,
                                            substring.transformed || extract.transformed,
                                        )
                                    },
                                )
                            })
                            .map_err(|error| {
                                datafusion_common::DataFusionError::External(Box::new(error))
                            })
                    })
                    .map(|expression| expression.update_data(|expression| name.restore(expression)))
            })
        })?
        .data)
}

// Restore conditional substring casts only after LIMIT pruning has produced
// an actual empty input. A false filter or empty provider is not that boundary.
fn lower_substrings_over_pruned_empty_input(node: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    let mut input: &LogicalPlan = match &node {
        LogicalPlan::Projection(project) => &project.input,
        LogicalPlan::Filter(filter) => &filter.input,
        _ => return Ok(Transformed::no(node)),
    };
    loop {
        input = match input {
            LogicalPlan::Projection(project) => &project.input,
            LogicalPlan::SubqueryAlias(alias) => &alias.input,
            LogicalPlan::Sort(sort) => &sort.input,
            LogicalPlan::Filter(filter) => &filter.input,
            LogicalPlan::EmptyRelation(empty) if !empty.produce_one_row => break,
            _ => return Ok(Transformed::no(node)),
        };
    }
    let names = datafusion_expr::expr_rewriter::NamePreserver::new(&node);
    node.map_expressions(|expression| {
        let name = names.save(&expression);
        expression
            .transform_up(|expression| {
                lower_runtime_substring(expression, true)
                    .map_err(|error| datafusion_common::DataFusionError::External(Box::new(error)))
            })
            .map(|expression| expression.update_data(|expression| name.restore(expression)))
    })
}

#[test]
fn substring_empty_input_lowering_keeps_boundaries_and_callbacks() -> Result<()> {
    use datafusion::arrow::datatypes::Field;
    use datafusion_common::{JoinType, Spans};
    use datafusion_expr::logical_plan::{EmptyRelation, Projection, Subquery};
    use datafusion_expr::{LogicalPlanBuilder, ScalarUDF, Volatility, cast, col, create_udf};
    use std::sync::atomic::{AtomicUsize, Ordering};

    let schema = Arc::new(DFSchema::try_from(Schema::new(vec![Field::new(
        "s",
        DataType::Utf8,
        true,
    )]))?);
    let relation = |produce_one_row| {
        LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row,
            schema: Arc::clone(&schema),
        })
    };
    let empty = relation(false);
    let alias = LogicalPlanBuilder::from(empty.clone())
        .project([col("s")])?
        .alias("t")?
        .build()?;
    let filtered = LogicalPlanBuilder::from(relation(true))
        .filter(lit(false))?
        .build()?;
    let aggregate = LogicalPlanBuilder::from(empty.clone())
        .aggregate(
            Vec::<Expr>::new(),
            [datafusion::functions_aggregate::expr_fn::count(lit(1))],
        )?
        .build()?;
    let left = LogicalPlanBuilder::from(empty.clone())
        .alias("left")?
        .build()?;
    let right = LogicalPlanBuilder::from(relation(true))
        .alias("right")?
        .build()?;
    let join = LogicalPlanBuilder::from(left)
        .join(
            right,
            JoinType::Full,
            (Vec::<String>::new(), Vec::<String>::new()),
            Some(lit(true)),
        )?
        .build()?;
    let subquery = LogicalPlan::Subquery(Subquery {
        subquery: Arc::new(empty.clone()),
        outer_ref_columns: vec![],
        spans: Spans::new(),
    });
    let filtered_aggregate = LogicalPlanBuilder::from(aggregate.clone())
        .filter(lit(false))?
        .build()?;
    let filtered_join = LogicalPlanBuilder::from(join.clone())
        .filter(lit(false))?
        .build()?;
    let filtered_subquery = LogicalPlanBuilder::from(subquery.clone())
        .filter(lit(false))?
        .build()?;
    let calls = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&calls);
    let callback = create_udf(
        "empty_substring_sibling",
        vec![],
        DataType::Int64,
        Volatility::Immutable,
        Arc::new(move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
            Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(7))))
        }),
    );
    let position = ScalarUDF::from(SparkCastNumericToInteger::try_new(
        DataType::Int32,
        true,
        false,
    )?)
    .call(vec![lit(f32::NAN)]);
    let substring = ScalarUDF::from(SparkSubstring::new().with_logical_arguments()).call(vec![
        lit("abcdef"),
        cast(position, DataType::Int64),
        lit(2_i64),
    ]);
    for (input, changes) in [
        (empty, true),
        (alias, true),
        (relation(true), false),
        (filtered, false),
        (aggregate, false),
        (join, false),
        (subquery, false),
        (filtered_aggregate, false),
        (filtered_join, false),
        (filtered_subquery, false),
    ] {
        let node = LogicalPlan::Projection(Projection::try_new(
            vec![
                substring.clone().alias("r"),
                callback.call(vec![]).alias("sibling"),
            ],
            Arc::new(input),
        )?);
        let prepared = lower_substrings_over_pruned_empty_input(node.clone())?;
        assert_eq!(prepared.transformed, changes);
        assert_eq!(prepared.data.schema(), node.schema());
        assert_eq!(prepared.data.inputs(), node.inputs());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        if !changes {
            assert_eq!(prepared.data, node);
        }
    }
    Ok(())
}

// Expression preparation holds these LIMIT-zero consumers for the later
// query-clock check. Release that retention once mandatory LIKE checks finish.
pub(crate) fn prune_retained_local_like_limits(
    plan: LogicalPlan,
    props: &ExecutionProps,
) -> Result<LogicalPlan> {
    let prepare = Prepare {
        props: props.clone(),
        has_groups: false,
        has_null_sensitive_comparison: false,
        spark_array_index_errors: false,
    };
    Ok(plan
        .transform_down_with_subqueries(|node| {
            if let LogicalPlan::Limit(limit) = &node
                && crate::like_errors::contains_early_local_like(&limit.input)?
            {
                prepare.prune_limit_zero(node, false)
            } else {
                Ok(Transformed::no(node))
            }
        })?
        .data)
}

fn forbids_local_planning(expr: &Expr) -> Result<bool> {
    expr.exists(|expr| {
        Ok(matches!(expr, Expr::ScalarFunction(function)
            if !function.func.inner().supports_constant_folding()))
    })
}

fn input_forbids_local_planning(input: &LogicalPlan) -> Result<bool> {
    input.exists(|node| {
        let mut forbidden = false;
        node.apply_expressions(|expr| {
            forbidden |= forbids_local_planning(expr)?;
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(forbidden)
    })
}

// Check errors using discarded local rows. True means local interpretation
// consumed this expression, including an empty input; false means ineligible.
// No table scan or volatile expression is executed; no rows enter the real plan.
pub(crate) fn try_check_local_row_errors(
    expr: &Expr,
    input: &LogicalPlan,
    props: &ExecutionProps,
) -> Result<bool> {
    if !input.exists(|node| Ok(matches!(node, LogicalPlan::Values(_))))? {
        return Ok(false);
    }
    if forbids_local_planning(expr)? || input_forbids_local_planning(input)? {
        return Ok(false);
    }
    // Spark resolves its query clock before local interpretation. Lower only
    // that native primitive; general folding could erase live local errors.
    let mut context = datafusion_expr::simplify::SimplifyContext::builder()
        .with_schema(Arc::clone(input.schema()))
        .with_query_execution_start_time(props.query_execution_start_time);
    if let Some(options) = &props.config_options {
        context = context.with_config_options(Arc::clone(options));
    }
    let info = context.build();
    let prepare_clock = |expr: Expr| {
        expr.transform_down(|expr| {
            if let Expr::ScalarFunction(function) = &expr
                && function.func.inner().is::<TimestampNow>()
                && let datafusion_expr::simplify::ExprSimplifyResult::Simplified(clock) =
                    function.func.simplify(function.args.clone(), &info)?
            {
                Ok(Transformed::yes(clock))
            } else {
                Ok(Transformed::no(expr))
            }
        })
    };
    let input = input
        .clone()
        .transform_up_with_subqueries(|node| node.map_expressions(prepare_clock))?
        .data;
    let prepare = Prepare {
        props: props.clone(),
        has_groups: false,
        has_null_sensitive_comparison: false,
        spark_array_index_errors: false,
    };
    let Some(batch) = prepare.local_batch(&input)? else {
        return Ok(false);
    };
    // Local projection/filter conversion consumes even an empty input.
    if batch.num_rows() == 0 {
        return Ok(true);
    }
    if !is_locally_evaluable(expr, true)? {
        return Ok(false);
    }
    let expr = prepare_clock(expr.clone())?.data;
    let expr = prepare.precheck_expression(expr, input.schema(), true)?;
    create_physical_expr(&expr, input.schema(), &prepare.props)?.evaluate(&batch)?;
    Ok(true)
}

// Only the two opted-in string implementations admit three closed arguments.
// Other higher-order functions retain their existing preparation.
fn local_string_argument_bodies(expr: &Expr) -> Result<Option<[&Expr; 3]>> {
    Ok(SparkSubstring::closed_argument_bodies(expr)?
        .or(SparkRegexpExtract::closed_argument_bodies(expr)?))
}

// Reuse the runtime lowerer's closed-literal admission without evaluating a body.
fn local_literal_regexp_extract(expr: &Expr) -> Result<bool> {
    let mut expr = without_metadata_annotations(expr);
    while let Expr::Alias(alias) = expr {
        expr = without_metadata_annotations(&alias.expr);
    }
    let Some(arguments) = SparkRegexpExtract::closed_argument_bodies(expr)? else {
        return Ok(false);
    };
    let Expr::HigherOrderFunction(function) = expr else {
        return Ok(false);
    };
    let Some(extract) = (function.func.inner().as_ref() as &dyn std::any::Any)
        .downcast_ref::<SparkRegexpExtract>()
        .filter(|extract| !extract.has_known_null_argument())
    else {
        return Ok(false);
    };
    let scalar = datafusion_expr::ScalarUDF::from(extract.clone())
        .call(arguments.into_iter().cloned().collect());
    Ok(!lower_runtime_regexp_extract(scalar, false)
        .map_err(|error| datafusion_common::DataFusionError::External(Box::new(error)))?
        .transformed)
}

// Keep the existing local folding permissions; async inputs require their planner.
fn literal_regexp_input_permitted(input: &LogicalPlan) -> Result<bool> {
    if !early_local_relation(input)? || !input_permits_array_index_preparation(input)? {
        return Ok(false);
    }
    let mut asynchronous = false;
    input.apply(|node| {
        node.apply_expressions(|expr| {
            asynchronous |= expr.exists(|expr| {
                Ok(matches!(expr, Expr::ScalarFunction(function) if function.func.as_async().is_some()))
            })?;
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(!asynchronous)
}

// Structural eligibility for local evaluation, not execution or guaranteed success.
// Evaluators must separately check permits_constant_preparation before invoking calls.
// Aggregates, subqueries and outer references need their own planning context;
// volatile calls remain in execution even when local columns are available.
pub(crate) fn is_locally_evaluable(expr: &Expr, allow_columns: bool) -> Result<bool> {
    is_locally_evaluable_with_volatility(expr, allow_columns, false)
}

fn is_locally_evaluable_with_volatility(
    expr: &Expr,
    allow_columns: bool,
    allow_volatile: bool,
) -> Result<bool> {
    let mut eligible = true;
    expr.apply(|node| {
        if let Some(bodies) = local_string_argument_bodies(node)? {
            for body in bodies {
                if !is_locally_evaluable_with_volatility(body, allow_columns, allow_volatile)? {
                    eligible = false;
                    return Ok(TreeNodeRecursion::Stop);
                }
            }
            return Ok(TreeNodeRecursion::Jump);
        }
        let unsupported = match node {
            Expr::Column(_) => !allow_columns,
            Expr::Literal(_, _)
            | Expr::Alias(_)
            | Expr::BinaryExpr(_)
            | Expr::Cast(_)
            | Expr::TryCast(_)
            | Expr::Case(_)
            | Expr::Negative(_)
            | Expr::Not(_)
            | Expr::IsNull(_)
            | Expr::IsNotNull(_)
            | Expr::IsTrue(_)
            | Expr::IsFalse(_)
            | Expr::IsUnknown(_)
            | Expr::IsNotTrue(_)
            | Expr::IsNotFalse(_)
            | Expr::IsNotUnknown(_)
            | Expr::Between(_)
            | Expr::InList(_)
            | Expr::Like(_)
            | Expr::SimilarTo(_) => false,
            Expr::ScalarFunction(_) => !allow_volatile && node.is_volatile_node(),
            _ => true,
        };
        if unsupported {
            eligible = false;
            Ok(TreeNodeRecursion::Stop)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    })?;
    Ok(eligible)
}

#[test]
fn local_literal_regexp_group_admission_reuses_runtime_routing() -> Result<()> {
    use datafusion_expr::expr::{HigherOrderFunction, Lambda};
    use datafusion_expr::{HigherOrderUDF, cast, col};

    let closed = |source, pattern, index, known_null| {
        Expr::HigherOrderFunction(HigherOrderFunction::new(
            Arc::new(HigherOrderUDF::new_from_impl(
                SparkRegexpExtract::new().with_null_short_circuit(known_null),
            )),
            vec![source, pattern, index]
                .into_iter()
                .map(|body| Expr::Lambda(Lambda::new(vec!["unused".into()], body)))
                .collect(),
        ))
    };
    for index in [-1_i32, 0, 2] {
        let expr = closed(
            lit("a"),
            lit("(a)"),
            cast(cast(lit(index), DataType::Int32), DataType::Int64),
            false,
        );
        assert!(local_literal_regexp_extract(&expr)?);
        assert!(local_literal_regexp_extract(&expr.alias("r"))?);
    }
    // Admission does not compile a pattern or evaluate a malformed index cast.
    let invalid = SparkCastStringToInteger::try_new(
        DataType::Int32,
        sail_function::scalar::spark_cast_string_to_integer::StringIntegerCastMode::Ansi,
    )?
    .into_expr(lit("bad"));
    assert!(local_literal_regexp_extract(&closed(
        lit("a"),
        lit("(a)"),
        cast(invalid, DataType::Int64),
        false,
    ))?);
    assert!(local_literal_regexp_extract(&closed(
        lit("a"),
        lit("["),
        lit(2_i64),
        false,
    ))?);
    for expr in [
        closed(lit(ScalarValue::Utf8(None)), lit("(a)"), lit(2_i64), true),
        closed(col("s"), lit("(a)"), lit(2_i64), false),
        closed(lit("a"), lit("(a)"), col("i"), false),
        closed(lit("a"), lit("(a)"), lit(1_i64) + lit(1_i64), false),
    ] {
        assert!(!local_literal_regexp_extract(&expr)?);
    }
    Ok(())
}

#[cfg(test)]
#[path = "test_utils/regexp_literal_producer.rs"]
mod literal_producer;

#[tokio::test]
async fn literal_regexp_producer_permissions_and_retention()
-> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use self::literal_producer::{Kind, producer};
    use crate::{config::PlanConfig, physical_plan::SparkQueryPlanner, resolver::PlanResolver};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::SessionContext;
    use datafusion_expr::expr::{HigherOrderFunction, Lambda};
    use datafusion_expr::{HigherOrderUDF, LogicalPlanBuilder};
    use std::sync::atomic::Ordering;

    let outer = Expr::HigherOrderFunction(HigherOrderFunction::new(
        Arc::new(HigherOrderUDF::new_from_impl(
            SparkRegexpExtract::new().with_null_short_circuit(false),
        )),
        vec![lit("a"), lit("(a)"), lit(1_i64)]
            .into_iter()
            .map(|body| Expr::Lambda(Lambda::new(vec!["unused".into()], body)))
            .collect(),
    ))
    .alias("r");
    assert!(local_literal_regexp_extract(&outer)?);
    for kind in [
        Kind::ImmutableNoFold,
        Kind::VolatileFold,
        Kind::AsyncFold,
        Kind::ImmutableFold,
    ] {
        let (callback, counts) = producer(kind);
        assert_eq!(
            callback.inner().supports_constant_folding(),
            kind != Kind::ImmutableNoFold
        );
        assert_eq!(
            callback.signature().volatility == datafusion_expr::Volatility::Volatile,
            kind == Kind::VolatileFold
        );
        assert_eq!(callback.as_async().is_some(), kind == Kind::AsyncFold);
        let values = LogicalPlanBuilder::values(vec![vec![lit(1_i32)]])?.build()?;
        let key = Expr::Column(values.schema().columns()[0].clone()).alias("k");
        let values = LogicalPlanBuilder::from(values)
            .project(vec![key])?
            .build()?;
        let column = values.schema().columns()[0].clone();
        let input = LogicalPlan::Projection(datafusion_expr::logical_plan::Projection::try_new(
            vec![callback.call(vec![Expr::Column(column)]).alias("produced")],
            Arc::new(values),
        )?);
        let input = LogicalPlan::SubqueryAlias(
            datafusion_expr::logical_plan::SubqueryAlias::try_new(Arc::new(input), "q")?,
        );
        assert_eq!(
            literal_regexp_input_permitted(&input)?,
            kind == Kind::ImmutableFold
        );
        assert_eq!(
            counts.calls.load(Ordering::SeqCst),
            0,
            "predicate invoked {kind:?}"
        );
    }

    for ansi_mode in [true, false] {
        let session = SessionContext::new_with_state(
            SessionStateBuilder::new()
                .with_default_features()
                .with_analyzer_rule(Arc::new(PrepareSparkExpressions))
                .with_query_planner(Arc::new(SparkQueryPlanner))
                .build(),
        );
        let (callback, counts) = producer(Kind::ImmutableFold);
        session.register_udf(callback);
        let sql = "SELECT regexp_extract('a','(a)',1) AS r, produced FROM \
            (SELECT literal_counted_producer(k) AS produced FROM VALUES(1) AS t(k)) AS q";
        let mut config = PlanConfig::default();
        config.ansi_mode = ansi_mode;
        let ast = sail_sql_analyzer::parser::parse_one_statement(sql)?;
        let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
        let named = PlanResolver::new(&session, Arc::new(config))
            .resolve_named_plan(spec)
            .await?;
        assert_eq!(
            counts.calls.load(Ordering::SeqCst),
            0,
            "resolve consumed the producer"
        );
        let resolved = format!("{:?}", named.plan.schema());
        let mut options = ConfigOptions::default();
        options
            .extensions
            .insert(SparkSqlExpressionSemantics::default());
        options
            .extensions
            .insert(SparkInterpretedEvaluation::default());
        let mut props = ExecutionProps::new();
        props.config_options = Some(Arc::new(options.clone()));
        let prepare = Prepare {
            props,
            has_groups: false,
            has_null_sensitive_comparison: false,
            spark_array_index_errors: true,
        };
        counts.phase.store(1, Ordering::SeqCst);
        let prepared = prepare.cast_reachability(
            preserve_local_string_arguments(named.plan)?,
            &options,
            false,
        )?;
        assert_eq!(
            counts.calls.load(Ordering::SeqCst),
            1,
            "input conversion must run once"
        );
        assert!(
            !prepared.exists(|node| {
                let mut original = false;
                node.apply_expressions(|expr| {
                    original |= expr.exists(|expr| {
                        Ok(matches!(expr,Expr::ScalarFunction(function)
                    if function.func.name() == "literal_counted_producer"))
                    })?;
                    Ok(TreeNodeRecursion::Continue)
                })?;
                Ok(original)
            })?,
            "retained plan still contains the original producer"
        );
        let retained_plan = format!("{prepared:?}");
        let physical = spark_test_frame(&session, prepared)
            .create_physical_plan()
            .await?;
        assert_eq!(
            counts.calls.load(Ordering::SeqCst),
            1,
            "later planning replayed the producer"
        );
        counts.phase.store(2, Ordering::SeqCst);
        let batches = datafusion::physical_plan::collect(physical, session.task_ctx()).await?;
        assert_eq!(
            counts.calls.load(Ordering::SeqCst),
            1,
            "collection replayed the producer"
        );
        assert_eq!(
            batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            1
        );
        assert_eq!(batches[0].num_columns(), 2);
        assert_eq!(
            ScalarValue::try_from_array(batches[0].column(0).as_ref(), 0)?,
            ScalarValue::Utf8(Some("a".into()))
        );
        assert_eq!(
            ScalarValue::try_from_array(batches[0].column(1).as_ref(), 0)?,
            ScalarValue::Int32(Some(1))
        );
        eprintln!(
            "c06 literal producer retention: {}",
            serde_json::json!({
            "sql":sql,"ansi":ansi_mode,"resolved_schema":resolved,"retained_plan":retained_plan,
            "counts":counts.snapshot(),"spark_oracle_credit":false})
        );
    }
    Ok(())
}

#[test]
fn substring_preparation_requires_closed_arguments_and_pure_bodies() -> Result<()> {
    use datafusion_expr::expr::{HigherOrderFunction, Lambda, LambdaVariable};
    use datafusion_expr::{HigherOrderUDF, Volatility, col, create_udf};
    use sail_function::scalar::datetime::convert_tz::{ConvertTz, ConvertTzLazy};

    let lambda = |body| Expr::Lambda(Lambda::new(vec!["unused".into()], body));
    let wrapper = |arguments| {
        Expr::HigherOrderFunction(HigherOrderFunction::new(
            Arc::new(HigherOrderUDF::new_from_impl(SparkSubstring::new())),
            arguments,
        ))
    };
    let closed = wrapper(vec![
        lambda(lit("abc")),
        lambda(lit(1_i64)),
        lambda(lit(2_i64)),
    ]);
    assert!(local_string_argument_bodies(&closed)?.is_some());
    assert!(is_locally_evaluable(&closed, false)?);
    let column = wrapper(vec![
        lambda(col("s")),
        lambda(lit(1_i64)),
        lambda(lit(2_i64)),
    ]);
    assert!(!is_locally_evaluable(&column, false)?);
    assert!(is_locally_evaluable(&column, true)?);
    for arguments in [
        vec![lambda(lit("abc")), lambda(lit(1_i64))],
        vec![lit("abc"), lambda(lit(1_i64)), lambda(lit(2_i64))],
        vec![
            lambda(Expr::LambdaVariable(LambdaVariable::new(
                "unused".into(),
                None,
            ))),
            lambda(lit(1_i64)),
            lambda(lit(2_i64)),
        ],
    ] {
        let malformed = wrapper(arguments);
        assert!(local_string_argument_bodies(&malformed)?.is_none());
        assert!(!is_locally_evaluable(&malformed, true)?);
    }
    let volatile = create_udf(
        "volatile_substring_source",
        vec![DataType::Utf8],
        DataType::Utf8,
        Volatility::Volatile,
        Arc::new(|args| Ok(args[0].clone())),
    );
    let volatile = wrapper(vec![
        lambda(volatile.call(vec![lit("abc")])),
        lambda(lit(1_i64)),
        lambda(lit(2_i64)),
    ]);
    assert!(local_string_argument_bodies(&volatile)?.is_some());
    assert!(!is_locally_evaluable(&volatile, true)?);
    assert!(is_locally_evaluable_with_volatility(&volatile, true, true)?);
    let other = Expr::HigherOrderFunction(HigherOrderFunction::new(
        Arc::new(HigherOrderUDF::new_from_impl(ConvertTzLazy::new(
            ConvertTz::new(false),
        ))),
        vec![lambda(lit("UTC")), lambda(lit("UTC")), lambda(lit(1_i64))],
    ));
    assert!(local_string_argument_bodies(&other)?.is_none());
    assert!(!is_locally_evaluable(&other, true)?);
    Ok(())
}

#[test]
fn substring_precheck_keeps_computed_null_source_constants_reachable() -> Result<()> {
    use datafusion::arrow::array::{ArrayRef, StringArray};
    use datafusion::arrow::error::ArrowError;
    use datafusion_common::DataFusionError;
    use datafusion_expr::{ScalarUDF, cast, col};
    use sail_function::scalar::collection::spark_concat::SparkConcat;

    let schema = Arc::new(DFSchema::try_from(Schema::new(vec![
        datafusion::arrow::datatypes::Field::new("s", DataType::Utf8, true),
    ]))?);
    let mut props = ExecutionProps::new();
    props.config_options = Some(Arc::new(ConfigOptions::default()));
    let prepare = Prepare {
        props,
        has_groups: false,
        has_null_sensitive_comparison: false,
        spark_array_index_errors: false,
    };
    let source = datafusion_expr::expr_fn::when(lit(true), lit(ScalarValue::Utf8(None)))
        .otherwise(col("s"))?;
    let bad_position = cast(lit(1_i64) / lit(0_i64), DataType::Int64);
    let marker = ScalarUDF::from(SparkSubstring::new().with_logical_arguments()).call(vec![
        source.clone(),
        bad_position.clone(),
        lit(2_i64),
    ]);
    let prepared = prepare.precheck_expression(marker.clone(), &schema, false)?;
    assert_eq!(prepared, marker);
    assert!(!is_locally_evaluable(&prepared, false)?);
    let concat = ScalarUDF::from(SparkConcat::new().with_ordered_evaluation());
    for args in [
        vec![lit(ScalarValue::Utf8(None)), prepared.clone()],
        vec![prepared.clone(), lit(ScalarValue::Utf8(None))],
    ] {
        let expression = prepare.precheck_expression(concat.call(args), &schema, false)?;
        let error = prepare.check_expr(&expression, true).unwrap_err();
        assert!(
            matches!(error.find_root(), DataFusionError::ArrowError(cause, _)
            if matches!(cause.as_ref(), ArrowError::DivideByZero))
        );
    }

    // Factory-known NULL and whole/local parent masking keep their own paths.
    let literal_null = ScalarUDF::from(
        SparkSubstring::new()
            .with_logical_arguments()
            .with_known_null_argument(true),
    )
    .call(vec![
        lit(ScalarValue::Utf8(None)),
        bad_position.clone(),
        lit(2_i64),
    ]);
    let literal_null = prepare.precheck_expression(literal_null, &schema, false)?;
    assert!(matches!(&literal_null, Expr::Literal(value, _) if value.is_null()));
    prepare.check_expr(&literal_null, true)?;

    let constant_source = datafusion_expr::expr_fn::when(lit(true), lit(ScalarValue::Utf8(None)))
        .otherwise(lit("abc"))?;
    let constant = ScalarUDF::from(SparkSubstring::new().with_logical_arguments()).call(vec![
        constant_source,
        bad_position,
        lit(2_i64),
    ]);
    let constant = lower_runtime_substring(constant, false)
        .map_err(|error| DataFusionError::External(Box::new(error)))?
        .data;
    let constant = prepare.precheck_expression(constant, &schema, false)?;
    prepare.check_expr(&constant, true)?;

    let local = lower_runtime_substring(marker, false)
        .map_err(|error| DataFusionError::External(Box::new(error)))?
        .data;
    let local = prepare.precheck_expression(local, &schema, true)?;
    let batch = RecordBatch::try_new(
        Arc::clone(schema.inner()),
        vec![Arc::new(StringArray::from(vec!["abc"])) as ArrayRef],
    )?;
    let value = create_physical_expr(&local, &schema, &prepare.props)?
        .evaluate(&batch)?
        .into_array(1)?;
    assert_eq!(value.null_count(), 1);
    Ok(())
}

// Track foldability per output instead of substituting producer expression trees.
// Repeated column references reuse one Boolean, including nonfoldable siblings.
fn case_one_row_foldability(
    plan: &LogicalPlan,
    input: Option<Vec<bool>>,
) -> Result<Option<Vec<bool>>> {
    match plan {
        LogicalPlan::EmptyRelation(empty)
            if empty.produce_one_row && empty.schema.fields().is_empty() =>
        {
            Ok(Some(vec![]))
        }
        LogicalPlan::SubqueryAlias(_) => Ok(input),
        LogicalPlan::Projection(project) => {
            let Some(input) = input else { return Ok(None) };
            project
                .expr
                .iter()
                .map(|expression| {
                    let mut foldable = is_locally_evaluable(expression, true)?;
                    expression.apply(|node| {
                        if let Expr::Column(column) = node {
                            foldable &= input[project.input.schema().index_of_column(column)?];
                        }
                        Ok(TreeNodeRecursion::Continue)
                    })?;
                    Ok(foldable)
                })
                .collect::<Result<Vec<_>>>()
                .map(Some)
        }
        _ => Ok(None),
    }
}

// Spark 4.2's LocalRelation early batch evaluates these VALUES consumers before
// ConstantFolding/SimplifyCasts/SimplifyCaseConversionExpressions in operator
// optimization (Optimizer.scala at 32f7299601108917fb01920a54e084595b7b3bf8).
// Mark case calls without evaluating rows, including volatile expressions.
fn preserve_local_case_conversions(plan: LogicalPlan) -> Result<LogicalPlan> {
    fn local_relation(plan: &LogicalPlan) -> Result<bool> {
        let expressions = |expressions: &[Expr], columns| {
            expressions.iter().try_fold(true, |local, expr| {
                Ok::<_, datafusion_common::DataFusionError>(
                    local && is_locally_evaluable_with_volatility(expr, columns, true)?,
                )
            })
        };
        match plan {
            LogicalPlan::Values(values) => values
                .values
                .iter()
                .try_fold(true, |local, row| Ok(local && expressions(row, false)?)),
            LogicalPlan::Projection(project) => {
                Ok(local_relation(&project.input)? && expressions(&project.expr, true)?)
            }
            LogicalPlan::SubqueryAlias(alias) => local_relation(&alias.input),
            LogicalPlan::Filter(filter) => Ok(local_relation(&filter.input)?
                && is_locally_evaluable_with_volatility(&filter.predicate, true, true)?),
            LogicalPlan::Limit(limit) => {
                let literal_count = |expr: &Expr| match expr {
                    Expr::Literal(ScalarValue::Int32(Some(n)), _) => *n >= 0,
                    Expr::Literal(ScalarValue::Int64(Some(n)), _) => *n >= 0,
                    // The preceding TypeCoercion analyzer widens SQL Int32 literals.
                    Expr::Cast(cast) if cast.field.data_type() == &DataType::Int64 => {
                        matches!(cast.expr.as_ref(), Expr::Literal(ScalarValue::Int32(Some(n)), _) if *n >= 0)
                    }
                    _ => false,
                };
                // Spark's early local batch stops at every explicit OFFSET, even zero.
                Ok(limit.skip.is_none()
                    && limit.fetch.as_deref().is_some_and(literal_count)
                    && local_relation(&limit.input)?)
            }
            _ => Ok(false),
        }
    }

    // Count ordinary and outer-reference consumers, including other CTE uses.
    // Names shared by distinct qualifiers conservatively keep the producer.
    let mut column_uses = std::collections::HashMap::new();
    plan.apply_with_subqueries(|node| {
        for expression in node.expressions() {
            expression.apply(|expression| {
                if let Expr::Column(column) | Expr::OuterReferenceColumn(_, column) = expression {
                    *column_uses.entry(column.name.clone()).or_insert(0usize) += 1;
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    // Each frame holds only the last child's output flags. Subqueries are visited
    // before inputs; only single-input Projection/Alias nodes consume these flags.
    let one_row_outputs = std::cell::RefCell::new(Vec::<Option<Vec<bool>>>::new());
    Ok(plan
        .transform_down_up_with_subqueries(
            |node| {
                one_row_outputs.borrow_mut().push(None);
                Ok(Transformed::no(node))
            },
            |node| {
                let input = one_row_outputs.borrow_mut().pop().flatten();
                let outputs = case_one_row_foldability(&node, input)?;
                let foldable_one_row = outputs
                    .as_ref()
                    .is_some_and(|outputs| outputs.iter().all(|v| *v));
                if let Some(parent) = one_row_outputs.borrow_mut().last_mut() {
                    *parent = outputs;
                }
                if local_relation(&node)? || foldable_one_row {
                    node.map_expressions(preserve_local_case_conversion)
                } else {
                    simplify_projected_case_conversions(node, &column_uses)
                }
            },
        )?
        .data)
}

#[test]
fn array_binding_lookup_does_not_expand_dependent_projection_chains() -> Result<()> {
    use datafusion::arrow::datatypes::Field;
    use datafusion::datasource::{MemTable, TableProvider, provider_as_source};
    use datafusion_expr::logical_plan::{Projection, SubqueryAlias};
    use datafusion_expr::{LogicalPlanBuilder, ScalarUDF, col};
    use sail_function::scalar::array::spark_array::SparkArray;

    let provider: Arc<dyn TableProvider> = Arc::new(MemTable::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
        vec![vec![]],
    )?);
    let scan =
        LogicalPlanBuilder::scan("bounds_source", provider_as_source(provider), None)?.build()?;
    let base = Arc::new(LogicalPlan::Projection(Projection::try_new(
        vec![lit(1_i32).alias("v")],
        Arc::new(scan),
    )?));
    let consumer = ScalarUDF::from(SparkArray::new()).call(vec![col("v")]);
    let node_count = |expr: &Expr| -> Result<usize> {
        let mut nodes = 0;
        expr.apply(|_| {
            nodes += 1;
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(nodes)
    };

    let depth = 10;
    let mut dependent = Arc::clone(&base);
    for _ in 0..depth {
        let alias = LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(dependent, "q")?);
        dependent = Arc::new(LogicalPlan::Projection(Projection::try_new(
            vec![(col("v") + col("v")).alias("v")],
            Arc::new(alias),
        )?));
    }
    let mut input_nodes = 0;
    dependent.apply(|node| {
        node.apply_expressions(|expr| {
            input_nodes += node_count(expr)?;
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(TreeNodeRecursion::Continue)
    })?;
    assert_eq!(input_nodes, depth * 4 + 2);
    let work = inline_closed_projection_bindings_for_array_check(consumer.clone(), &dependent)?;
    assert!(
        node_count(&work)? <= input_nodes,
        "dependent projections expanded into the work copy"
    );
    assert_eq!(work, consumer, "dependent computations must remain opaque");

    let alias_depth = 24;
    let mut aliases = base;
    for _ in 0..alias_depth {
        let alias = LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(aliases, "q")?);
        aliases = Arc::new(LogicalPlan::Projection(Projection::try_new(
            vec![col("v").alias("v")],
            Arc::new(alias),
        )?));
    }
    let closed = inline_closed_projection_bindings_for_array_check(consumer, &aliases)?;
    assert!(!closed.any_column_refs());
    assert!(node_count(&closed)? <= alias_depth + 4);
    let mut values = Vec::new();
    closed.apply(|expr| {
        if let Expr::Literal(value, _) = expr {
            values.push(value.clone());
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    assert_eq!(values, vec![ScalarValue::Int32(Some(1))]);
    Ok(())
}

#[test]
fn case_foldability_does_not_expand_projection_chains()
-> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use std::sync::{
        LazyLock,
        atomic::{AtomicUsize, Ordering},
    };

    use datafusion::prelude::SessionContext;
    use datafusion_expr::logical_plan::{EmptyRelation, Projection, SubqueryAlias};
    use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
    use datafusion_expr::{
        ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility, col,
    };

    use crate::config::PlanConfig;
    use crate::function::{
        common::{FunctionContextInput, ScalarFunctionInput},
        get_built_in_function,
    };

    static CHECKS: AtomicUsize = AtomicUsize::new(0);
    #[derive(Debug, PartialEq, Eq, Hash)]
    struct FoldabilityProbe(bool);
    impl ScalarUDFImpl for FoldabilityProbe {
        fn name(&self) -> &str {
            "case_foldability_probe"
        }
        fn signature(&self) -> &Signature {
            static IMMUTABLE: LazyLock<Signature> =
                LazyLock::new(|| Signature::any(1, Volatility::Immutable));
            static VOLATILE: LazyLock<Signature> =
                LazyLock::new(|| Signature::any(1, Volatility::Volatile));
            CHECKS.fetch_add(1, Ordering::SeqCst);
            if self.0 { &VOLATILE } else { &IMMUTABLE }
        }
        fn return_type(&self, _: &[DataType]) -> Result<DataType> {
            Ok(DataType::Utf8)
        }
        fn invoke_with_args(&self, _: ScalarFunctionArgs) -> Result<ColumnarValue> {
            datafusion_common::internal_err!("foldability classification must not execute values")
        }
    }

    let session = SessionContext::new();
    let config = Arc::new(PlanConfig::default());
    let depth = 8;
    for volatile in [false, true] {
        for case_consumer in [false, true] {
            let source = LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: true,
                schema: Arc::new(DFSchema::empty()),
            });
            let mut plan = LogicalPlan::Projection(Projection::try_new(
                vec![
                    ScalarUDF::from(FoldabilityProbe(volatile))
                        .call(vec![lit("Straße")])
                        .alias("x"),
                ],
                Arc::new(source),
            )?);
            for _ in 0..depth {
                let input =
                    LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(Arc::new(plan), "q")?);
                plan = LogicalPlan::Projection(Projection::try_new(
                    vec![expr_fn::coalesce(vec![col("x"), col("x")]).alias("x")],
                    Arc::new(input),
                )?);
            }
            if case_consumer {
                let make_case = |name, argument| {
                    get_built_in_function(name)?(ScalarFunctionInput {
                        arguments: vec![argument],
                        function_context: FunctionContextInput {
                            argument_display_names: &[],
                            plan_config: &config,
                            session_context: &session,
                            schema: plan.schema(),
                        },
                    })
                };
                let expression = make_case("lower", make_case("upper", col("x"))?)?;
                plan = LogicalPlan::Projection(Projection::try_new(
                    vec![expression.alias("r")],
                    Arc::new(plan),
                )?);
            }
            CHECKS.store(0, Ordering::SeqCst);
            let prepared = preserve_local_case_conversions(plan)?;
            assert!(
                CHECKS.load(Ordering::SeqCst) <= depth + 1,
                "classification expanded producer expressions"
            );
            let LogicalPlan::Projection(project) = &prepared else {
                panic!("projection changed")
            };
            let info = SimplifyContext::builder()
                .with_schema(Arc::clone(project.input.schema()))
                .build();
            let mut consumers = 0;
            project.expr[0].apply(|expression| {
                if let Expr::ScalarFunction(function) = expression
                    && function.func.name() == "lower"
                {
                    consumers += 1;
                    let preserved = matches!(
                        function.func.simplify(function.args.clone(), &info)?,
                        ExprSimplifyResult::Original(_)
                    );
                    assert_eq!(preserved, !volatile, "one-row case foldability changed");
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
            assert_eq!(consumers, usize::from(case_consumer));
        }
    }
    Ok(())
}

// Structural admission for Spark's early local conversion. This does not
// execute volatile functions; is_locally_evaluable still excludes them in evaluators.
pub(crate) fn early_local_relation(plan: &LogicalPlan) -> Result<bool> {
    let expressions = |expressions: &[Expr], columns| {
        expressions.iter().try_fold(true, |local, expr| {
            Ok::<_, datafusion_common::DataFusionError>(
                local && is_locally_evaluable_with_volatility(expr, columns, true)?,
            )
        })
    };
    match plan {
        LogicalPlan::Values(values) => values
            .values
            .iter()
            .try_fold(true, |local, row| Ok(local && expressions(row, false)?)),
        LogicalPlan::Projection(project) => {
            Ok(early_local_relation(&project.input)? && expressions(&project.expr, true)?)
        }
        LogicalPlan::SubqueryAlias(alias) => early_local_relation(&alias.input),
        LogicalPlan::Filter(filter) => Ok(early_local_relation(&filter.input)?
            && is_locally_evaluable_with_volatility(&filter.predicate, true, true)?),
        LogicalPlan::Limit(limit) => {
            let literal_count = |expr: &Expr| match expr {
                Expr::Literal(ScalarValue::Int32(Some(n)), _) => *n >= 0,
                Expr::Literal(ScalarValue::Int64(Some(n)), _) => *n >= 0,
                Expr::Cast(cast) if cast.field.data_type() == &DataType::Int64 => {
                    matches!(cast.expr.as_ref(), Expr::Literal(ScalarValue::Int32(Some(n)), _) if *n >= 0)
                }
                _ => false,
            };
            Ok(limit.skip.is_none()
                && limit.fetch.as_deref().is_some_and(literal_count)
                && early_local_relation(&limit.input)?)
        }
        _ => Ok(false),
    }
}

// Inline projection/alias chains over OneRowRelation without evaluating them.
// Spark performs this before optimizing the remaining scalar subqueries.
fn one_row_exprs(plan: &LogicalPlan) -> Result<Option<Vec<Expr>>> {
    match plan {
        LogicalPlan::EmptyRelation(empty)
            if empty.produce_one_row && empty.schema.fields().is_empty() =>
        {
            Ok(Some(vec![]))
        }
        LogicalPlan::SubqueryAlias(alias) => one_row_exprs(&alias.input),
        LogicalPlan::Projection(project) => {
            let Some(input) = one_row_exprs(&project.input)? else {
                return Ok(None);
            };
            project
                .expr
                .iter()
                .map(|expr| {
                    Ok(expr
                        .clone()
                        .unalias()
                        .transform_up(|expr| match expr {
                            Expr::Column(column) => Ok(Transformed::yes(
                                input[project.input.schema().index_of_column(&column)?].clone(),
                            )),
                            Expr::OuterReferenceColumn(_, column) => {
                                Ok(Transformed::yes(Expr::Column(column)))
                            }
                            _ => Ok(Transformed::no(expr)),
                        })?
                        .data)
                })
                .collect::<Result<Vec<_>>>()
                .map(Some)
        }
        _ => Ok(None),
    }
}

fn has_cast(expr: &Expr) -> Result<bool> {
    expr.exists(|e| Ok(matches!(e, Expr::Cast(_))
        || matches!(e, Expr::ScalarFunction(f) if f.func.inner().downcast_ref::<SparkCastNumericToInteger>()
            .is_some_and(|cast| !cast.is_try()))
        || matches!(e, Expr::ScalarFunction(f) if f.func.inner().is::<sail_function::scalar::spark_cast_string_to_float::SparkCastStringToFloat>()
            || f.func.inner().is::<SparkCastStringToDecimal>()
            || f.func.inner().is::<SparkDecimalCast>()
            || f.func.inner().is::<SparkCastStringToBoolean>()
                || f.func.inner().is::<SparkCastStringToInteger>()
                || f.func.inner().is::<SparkStringTemporalComparisonCast>()
                || f.func.inner().is::<SparkNullIf>())))
}

// Preserve fallible string and Decimal casts before local pruning.
fn early_cast(expr: &Expr, schema: &DFSchemaRef, spark_comparison_errors: bool) -> Result<bool> {
    expr.exists(|expr| match expr {
        Expr::BinaryExpr(binary)
            if spark_comparison_errors
                && matches!(binary.op, Operator::Eq | Operator::NotEq | Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq)
                && null_literal(&binary.right) =>
        {
            is_locally_evaluable(&binary.left, true)
        }
        Expr::Cast(cast) => {
            if null_literal(&cast.expr) || !is_locally_evaluable(&cast.expr, true)? {
                return Ok(false);
            }
            let source = cast.expr.get_type(schema)?;
            Ok(source.is_string()
                || matches!((source, cast.field.data_type()),
                    (DataType::Decimal128(_, _), DataType::Decimal128(_, _))))
        }
        Expr::ScalarFunction(f)
            if f.func.inner().downcast_ref::<SparkDecimalCast>()
                .is_some_and(|cast| cast.mode() == DecimalCastMode::Ansi) =>
        {
            Ok(matches!(f.args.as_slice(), [expr]
                if !null_literal(expr) && is_locally_evaluable(expr, true)?
                    && matches!(expr.get_type(schema)?, DataType::Decimal128(_, _) | DataType::Decimal256(_, _))))
        }
        Expr::ScalarFunction(f)
            if f.func.inner().is::<SparkNullIf>() =>
        {
            let types = f.args.iter().map(|arg| arg.get_type(schema)).collect::<Result<Vec<_>>>()?;
            Ok(types.iter().any(DataType::is_string) && !types.iter().all(DataType::is_string))
        }
        Expr::ScalarFunction(f)
            if f.func
                .inner()
                .is::<sail_function::scalar::spark_cast_string_to_float::SparkCastStringToFloat>(
            ) || f.func.inner().is::<SparkCastStringToDecimal>()
                || f.func.inner().is::<SparkCastStringToBoolean>()
                || f.func.inner().is::<SparkCastStringToInteger>()
                || f.func.inner().is::<SparkStringTemporalComparisonCast>() =>
        {
            Ok(matches!(f.args.as_slice(), [expr] if !null_literal(expr) && is_locally_evaluable(expr, true)?))
        }
        _ => Ok(false),
    })
}

fn known_empty(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Filter(filter) => {
            matches!(
                filter.predicate,
                Expr::Literal(ScalarValue::Boolean(Some(false) | None), _)
            ) || known_empty(&filter.input)
        }
        LogicalPlan::Projection(project) => known_empty(&project.input),
        LogicalPlan::SubqueryAlias(alias) => known_empty(&alias.input),
        LogicalPlan::Sort(sort) => known_empty(&sort.input),
        LogicalPlan::EmptyRelation(empty) => !empty.produce_one_row,
        LogicalPlan::Values(values) => values.values.is_empty(),
        _ => false,
    }
}

impl Prepare {
    // Fold closed replacement calls before an outer LIMIT can discard local VALUES rows.
    fn prepare_local_regexp_replacements(
        &self,
        mut node: LogicalPlan,
        options: &ConfigOptions,
    ) -> Result<Transformed<LogicalPlan>> {
        let names = datafusion_expr::expr_rewriter::NamePreserver::new(&node);
        let LogicalPlan::Projection(project) = &mut node else {
            return Ok(Transformed::no(node));
        };
        let eligible = |expr: &Expr| -> Result<bool> {
            Ok(is_locally_evaluable(expr, false)?
                && permits_constant_preparation(expr)?
                && !expr.exists(|expr| {
                    Ok(matches!(expr, Expr::ScalarFunction(function)
                        if function.func.as_async().is_some()))
                })?
                && expr.exists(|expr| {
                    Ok(matches!(expr, Expr::ScalarFunction(function)
                        if function.func.inner().downcast_ref::<SparkRegexpReplace>()
                            .is_some_and(SparkRegexpReplace::prepares_literal_errors)))
                })?)
        };
        if !project
            .input
            .exists(|plan| Ok(matches!(plan, LogicalPlan::Values(_))))?
            || !literal_regexp_input_permitted(&project.input)?
            || !project.expr.iter().try_fold(false, |found, expr| {
                Ok::<_, datafusion_common::DataFusionError>(found || eligible(expr)?)
            })?
        {
            return Ok(Transformed::no(node));
        }
        // Native type admission precedes NULL folding or empty-input pruning.
        for expr in &project.expr {
            if eligible(expr)? {
                let _ = create_physical_expr(expr, project.input.schema(), &self.props)?;
            }
        }
        let input = self
            .simplify_local_input_expressions(Arc::unwrap_or_clone(Arc::clone(&project.input)))?;
        project.input = Arc::new(input.data);
        if known_empty(&project.input) {
            return Ok(Transformed::yes(LogicalPlan::EmptyRelation(
                datafusion_expr::logical_plan::EmptyRelation {
                    produce_one_row: false,
                    schema: Arc::clone(&project.schema),
                },
            )));
        }
        let info = datafusion_expr::simplify::SimplifyContext::builder()
            .with_schema(Arc::clone(project.input.schema()))
            .with_query_execution_start_time(self.props.query_execution_start_time)
            .with_config_options(Arc::new(options.clone()))
            .build();
        let simplifier = ExprSimplifier::new(info).with_local_input_errors();
        let mut changed = input.transformed;
        for expr in &mut project.expr {
            if eligible(expr)? {
                let name = names.save(expr);
                let (prepared, _) =
                    simplifier.simplify_with_cycle_count_transformed(expr.clone())?;
                changed |= prepared.transformed;
                *expr = name.restore(prepared.data);
            }
        }
        Ok(Transformed::new_transformed(
            node.recompute_schema()?,
            changed,
        ))
    }

    // Cache arguments after input checks, before current-expression previews can replay producers.
    fn prepare_array_errors_in_string_calls(
        &self,
        node: LogicalPlan,
    ) -> Result<Transformed<LogicalPlan>> {
        if known_empty(&node)
            || !node.inputs().iter().try_fold(true, |allowed, input| {
                Ok::<_, datafusion_common::DataFusionError>(
                    allowed && input_permits_array_index_preparation(input)?,
                )
            })?
        {
            return Ok(Transformed::no(node));
        }
        let schema = datafusion_expr::utils::merge_schema(&node.inputs());
        let names = datafusion_expr::expr_rewriter::NamePreserver::new(&node);
        node.map_expressions(|expr| {
            let name = names.save(&expr);
            self.prepare_array_errors_in_string_expression(expr, &schema)
                .map(|prepared| prepared.update_data(|expr| name.restore(expr)))
        })
    }

    fn prepare_array_errors_in_string_expression(
        &self,
        expr: Expr,
        schema: &DFSchema,
    ) -> Result<Transformed<Expr>> {
        expr.transform_down(|expr| {
            let string_call = local_string_argument_bodies(&expr)?.is_some();
            if null_discarded(&expr) || (expr.short_circuits() && !string_call) {
                return Ok(Transformed::new(expr, false, TreeNodeRecursion::Jump));
            }
            if !string_call {
                return Ok(Transformed::no(expr));
            }
            if !array_index_error_candidate(&expr)?
                || expr.exists(|expr| {
                    Ok(matches!(expr, Expr::ScalarFunction(function)
                    if !function.func.inner().supports_constant_folding()))
                })?
            {
                return Ok(Transformed::new(expr, false, TreeNodeRecursion::Jump));
            }
            let Expr::HigherOrderFunction(mut function) = expr else {
                return Ok(Transformed::no(expr));
            };
            let mut changed = false;
            for argument in &mut function.args {
                let Expr::Lambda(lambda) = argument else {
                    continue;
                };
                if null_literal(&lambda.body) {
                    break;
                }
                let prepared = self.prepare_array_errors_in_string_expression(
                    *std::mem::take(&mut lambda.body),
                    schema,
                )?;
                changed |= prepared.transformed;
                *lambda.body = prepared.data;
                if !is_locally_evaluable(&lambda.body, false)?
                    || !permits_constant_preparation(&lambda.body)?
                {
                    if self.string_result_is_null_on_success(&lambda.body, schema)? {
                        break;
                    }
                    continue;
                }
                let (_, field) = lambda.body.to_field(schema)?;
                let value = match self.eval_constant(&lambda.body)? {
                    ColumnarValue::Scalar(value) => value,
                    ColumnarValue::Array(array) if array.len() == 1 => {
                        ScalarValue::try_from_array(array.as_ref(), 0)?
                    }
                    _ => {
                        return datafusion_common::internal_err!(
                            "closed string argument returned a result other than one row"
                        );
                    }
                };
                let is_null = value.is_null();
                let metadata =
                    (!field.metadata().is_empty()).then(|| FieldMetadata::from(field.as_ref()));
                let prepared = Expr::Literal(value, metadata);
                changed |= prepared != *lambda.body;
                *lambda.body = prepared;
                if is_null {
                    break;
                }
            }
            Ok(Transformed::new(
                Expr::HigherOrderFunction(function),
                changed,
                TreeNodeRecursion::Jump,
            ))
        })
    }

    // NULL on successful completion does not permit dropping earlier fallible or volatile work.
    fn string_result_is_null_on_success(&self, expr: &Expr, schema: &DFSchema) -> Result<bool> {
        let expr = without_metadata_annotations(expr);
        if null_literal(expr) {
            return Ok(true);
        }
        match expr {
            Expr::Alias(alias) => self.string_result_is_null_on_success(&alias.expr, schema),
            Expr::Cast(cast) => self.string_result_is_null_on_success(&cast.expr, schema),
            Expr::TryCast(cast) => self.string_result_is_null_on_success(&cast.expr, schema),
            Expr::Negative(input) | Expr::Not(input) => {
                self.string_result_is_null_on_success(input, schema)
            }
            Expr::BinaryExpr(binary) if binary.op.returns_null_on_null() => Ok(self
                .string_result_is_null_on_success(&binary.left, schema)?
                || self.string_result_is_null_on_success(&binary.right, schema)?),
            Expr::Like(like) | Expr::SimilarTo(like) => Ok(self
                .string_result_is_null_on_success(&like.expr, schema)?
                || self.string_result_is_null_on_success(&like.pattern, schema)?),
            Expr::ScalarFunction(function)
                if function.func.inner().is::<SparkCastStringToInteger>()
                    || function.func.inner().is::<SparkCastNumericToInteger>()
                    || function.func.inner().is::<SparkCastStringToFloat>()
                    || function.func.inner().is::<SparkCastStringToDecimal>()
                    || function.func.inner().is::<SparkCastStringToBoolean>()
                    || function.func.inner().is::<SparkDecimalCast>()
                    || function.func.inner().is::<SparkToUtf8>()
                    || function.func.inner().is::<SparkToLargeUtf8>()
                    || function.func.inner().is::<SparkToUtf8View>()
                    || function.func.inner().is::<SparkNegative>() =>
            {
                match function.args.as_slice() {
                    [input] => self.string_result_is_null_on_success(input, schema),
                    _ => Ok(false),
                }
            }
            Expr::ScalarFunction(function) => {
                let inner = function.func.inner();
                let mut proof = function.clone();
                let mut all_null = !proof.args.is_empty();
                for (index, argument) in proof.args.iter_mut().enumerate() {
                    if self.string_result_is_null_on_success(argument, schema)? {
                        if inner.short_circuits_on_any_null_argument()
                            || (index == 0 && inner.short_circuits_on_null_first_arg())
                            || (index == 1 && inner.is::<SparkLike>())
                        {
                            return Ok(true);
                        }
                        *argument = Expr::Literal(ScalarValue::Null, None);
                    } else {
                        all_null = false;
                    }
                }
                Ok((inner.returns_first_non_null_argument() && all_null)
                    || null_discarded(&Expr::ScalarFunction(proof)))
            }
            Expr::Case(case) => {
                let mut all_null = true;
                for (_, result) in &case.when_then_expr {
                    if !self.string_result_is_null_on_success(result, schema)? {
                        all_null = false;
                        break;
                    }
                }
                if all_null
                    && case.else_expr.as_deref().map_or(Ok(true), |result| {
                        self.string_result_is_null_on_success(result, schema)
                    })?
                {
                    return Ok(true);
                }
                let info = datafusion_expr::simplify::SimplifyContext::builder()
                    .with_schema(Arc::new(schema.clone()))
                    .with_config_options(self.props.config_options.clone().unwrap_or_default())
                    .build();
                let prepared = ExprSimplifier::new(info).simplify_algebraic_once(expr.clone())?;
                if prepared.transformed {
                    self.string_result_is_null_on_success(&prepared.data, schema)
                } else {
                    Ok(false)
                }
            }
            _ => {
                if let Some(bodies) = local_string_argument_bodies(expr)? {
                    for body in bodies {
                        if self.string_result_is_null_on_success(body, schema)? {
                            return Ok(true);
                        }
                    }
                }
                Ok(false)
            }
        }
    }

    fn simplify_projected_null_comparisons(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        // Expose only known NULLs across aliases; leave other expressions in place
        // so pruning cannot duplicate them or change their evaluation order.
        Ok(plan
            .transform_up(|node| {
                // Aggregate arguments and sort keys prepare their inputs first.
                let input = Arc::clone(match &node {
                    LogicalPlan::Projection(project) => &project.input,
                    LogicalPlan::Filter(filter) => &filter.input,
                    LogicalPlan::Window(window) => &window.input,
                    LogicalPlan::Aggregate(aggregate) => &aggregate.input,
                    _ => return Ok(Transformed::no(node)),
                });
                let info = datafusion_expr::simplify::SimplifyContext::builder()
                    .with_schema(Arc::clone(input.schema()))
                    .with_config_options(Arc::clone(self.props.config_options.as_ref().unwrap()))
                    .build();
                let names = datafusion_expr::expr_rewriter::NamePreserver::new(&node);
                let mut rewrite_expr = |expr: Expr| {
                    if expr.exists(|expr| Ok(matches!(expr, Expr::AggregateFunction(_))))? {
                        return Ok(Transformed::no(expr));
                    }
                    let name = names.save(&expr);
                    expr.transform_down(|expr| {
                        if let Expr::ScalarFunction(f) = &expr
                            && (f.func.inner().is::<ArrayContains>()
                                || f.func.inner().is::<SparkNullIf>())
                            && !expr.contains_scalar_subquery()
                        {
                            let args = f
                                .args
                                .iter()
                                .map(|arg| {
                                    if projected_null(arg, &input)? {
                                        Ok(lit(ScalarValue::try_new_null(
                                            &arg.get_type(input.schema())?,
                                        )?))
                                    } else {
                                        Ok(arg.clone())
                                    }
                                })
                                .collect::<Result<Vec<_>>>()?;
                            if f.func.inner().is::<SparkNullIf>() {
                                return Ok(match f.func.simplify(args, &info)? {
                                    datafusion_expr::simplify::ExprSimplifyResult::Simplified(
                                        expr,
                                    ) => Transformed::yes(expr),
                                    datafusion_expr::simplify::ExprSimplifyResult::Original(
                                        args,
                                    ) => Transformed::no(f.func.call(args)),
                                });
                            }
                            return prune_null(f.func.call(args), &info);
                        }
                        Ok(Transformed::no(expr))
                    })
                    .map(|expr| expr.update_data(|expr| name.restore(expr)))
                };
                node.map_expressions(|expr| {
                    if matches!(expr, Expr::GroupingSet(_)) {
                        expr.map_children(&mut rewrite_expr)
                    } else {
                        rewrite_expr(expr)
                    }
                })?
                .transform_data(|plan| {
                    prepare_foldable_groups(plan, self.props.config_options.as_ref().unwrap())
                })
            })?
            .data)
    }

    fn prune_projections(
        &self,
        plan: LogicalPlan,
        config: &OptimizerContext,
        exists: bool,
    ) -> Result<LogicalPlan> {
        let plan = if self.has_null_sensitive_comparison {
            // Native join rules decide which side can discard a failing input.
            let plan = plan.transform_down(|node| {
                if let LogicalPlan::Join(join) = &node
                    && let Some(predicate) = &join.filter
                    && predicate.exists(|expr| {
                        Ok(matches!(expr, Expr::ScalarFunction(f) if f.func.inner().is::<ArrayContains>()))
                    })?
                {
                    PushDownFilter::new().rewrite(node, config)
                } else {
                    Ok(Transformed::no(node))
                }
            })?.data;
            self.simplify_projected_null_comparisons(plan)?
        } else {
            plan
        };
        let plan = OptimizeProjections::new().rewrite(plan, config)?.data;
        if !self.has_groups {
            return Ok(plan);
        }
        let prepared = self.prepare_group_limits(plan, exists, exists)?;
        Ok(if prepared.transformed {
            OptimizeProjections::new()
                .rewrite(prepared.data, config)?
                .data
        } else {
            prepared.data
        })
    }

    // Column pruning can make an aggregate group-only. Only then can LIMIT 1
    // discard grouping work while preserving live aggregate/HAVING expressions.
    fn prepare_group_limits(
        &self,
        plan: LogicalPlan,
        one_row: bool,
        unused: bool,
    ) -> Result<Transformed<LogicalPlan>> {
        let one_row = match &plan {
            LogicalPlan::Projection(_) | LogicalPlan::SubqueryAlias(_) => one_row,
            LogicalPlan::Filter(filter)
                if matches!(
                    filter.predicate,
                    Expr::Literal(ScalarValue::Boolean(Some(true)), _)
                ) =>
            {
                one_row
            }
            LogicalPlan::Limit(limit) => {
                self.limit_count(limit.fetch.as_deref(), usize::MAX)?
                    .is_some_and(|count| count > 0 && (one_row || count == 1))
                    && self.limit_count(limit.skip.as_deref(), 0)? == Some(0)
            }
            LogicalPlan::Aggregate(_) => one_row,
            _ => false,
        };
        if one_row && unused
            && let LogicalPlan::Aggregate(aggregate) = &plan
            && aggregate.aggr_expr.is_empty()
            && !aggregate.group_expr.is_empty()
            && !aggregate.group_expr.iter().any(|expr| matches!(expr, Expr::GroupingSet(_)))
            // Spark's local relation conversion keeps these early errors.
            && self.local_batch(&aggregate.input)?.is_none()
        {
            let input = self
                .prepare_group_limits(
                    Arc::unwrap_or_clone(Arc::clone(&aggregate.input)),
                    true,
                    true,
                )?
                .data;
            // Sorting below a discarded group-only aggregate cannot affect row
            // presence. Keep limits, windows and other cardinality boundaries.
            let input = input
                .transform_down(|node| match node {
                    LogicalPlan::Sort(sort) => {
                        Ok(Transformed::yes(Arc::unwrap_or_clone(sort.input)))
                    }
                    LogicalPlan::Projection(_)
                    | LogicalPlan::SubqueryAlias(_)
                    | LogicalPlan::Filter(_) => Ok(Transformed::no(node)),
                    node => Ok(Transformed::new(node, false, TreeNodeRecursion::Jump)),
                })?
                .data;
            let input = LogicalPlan::Limit(datafusion_expr::logical_plan::Limit {
                skip: None,
                fetch: Some(Box::new(lit(1_i64))),
                input: Arc::new(input),
            });
            let projection = datafusion_expr::logical_plan::Projection::try_new_with_schema(
                aggregate.group_expr.clone(),
                Arc::new(input),
                Arc::clone(&aggregate.schema),
            )?;
            return Ok(Transformed::yes(LogicalPlan::Projection(projection)));
        }
        let unused = match &plan {
            LogicalPlan::Projection(project) => {
                unused
                    || project
                        .expr
                        .iter()
                        .all(|expr| !expr.any_column_refs() && !expr.contains_outer())
            }
            LogicalPlan::SubqueryAlias(_) | LogicalPlan::Limit(_) | LogicalPlan::Union(_) => unused,
            LogicalPlan::Filter(filter) => {
                unused && !filter.predicate.any_column_refs() && !filter.predicate.contains_outer()
            }
            LogicalPlan::Aggregate(aggregate) => {
                aggregate.group_expr.is_empty()
                    && aggregate
                        .aggr_expr
                        .iter()
                        .all(|expr| !expr.any_column_refs() && !expr.contains_outer())
            }
            _ => false,
        };
        let one_row = one_row && !matches!(plan, LogicalPlan::Aggregate(_));
        plan.map_children(|child| self.prepare_group_limits(child, one_row, unused))
    }
}

impl Prepare {
    fn identity_projection(project: &datafusion_expr::logical_plan::Projection) -> bool {
        project.expr.len() == project.input.schema().fields().len()
            && project.expr.iter().zip(project.input.schema().iter()).all(|(expr, (qualifier, field))| {
                // Sail adds one output alias; a SQL rename is another alias.
                let expr = if let Expr::Alias(alias) = expr { alias.expr.as_ref() } else { expr };
                matches!(expr, Expr::Column(column) if column.relation.as_ref() == qualifier && column.name == *field.name())
            })
    }

    // Spark's early LIMIT 1 rule crosses at most one nonidentity projection.
    fn prune_first_distinct(
        &self,
        plan: LogicalPlan,
        mut projections: usize,
    ) -> Result<Transformed<LogicalPlan>> {
        plan.transform_down(|node| {
            if let LogicalPlan::Distinct(datafusion_expr::logical_plan::Distinct::All(input)) = node
            {
                return Ok(Transformed::new(
                    self.prepare_distinct_limits(Arc::unwrap_or_clone(input), 0)?
                        .data,
                    true,
                    TreeNodeRecursion::Jump,
                ));
            }
            let recurse = match &node {
                LogicalPlan::Projection(project) if Self::identity_projection(project) => {
                    TreeNodeRecursion::Continue
                }
                LogicalPlan::Projection(_) if projections > 0 => {
                    projections -= 1;
                    TreeNodeRecursion::Continue
                }
                LogicalPlan::SubqueryAlias(_) => TreeNodeRecursion::Continue,
                LogicalPlan::Limit(limit)
                    if limit.skip.is_none()
                        && self.limit_count(limit.fetch.as_deref(), usize::MAX)? == Some(1) =>
                {
                    TreeNodeRecursion::Continue
                }
                _ => TreeNodeRecursion::Jump,
            };
            Ok(Transformed::new(node, false, recurse))
        })
    }

    fn prepare_distinct_limits(
        &self,
        plan: LogicalPlan,
        projections: usize,
    ) -> Result<Transformed<LogicalPlan>> {
        // Spark pushes deterministic parent projections below LIMIT before
        // checking the one-projection pattern, without collapsing them yet.
        let projections = match &plan {
            LogicalPlan::Projection(project) if Self::identity_projection(project) => projections,
            LogicalPlan::Projection(project)
                if project.expr.iter().all(|expr| !expr.is_volatile()) =>
            {
                projections + 1
            }
            LogicalPlan::SubqueryAlias(_) | LogicalPlan::Limit(_) => projections,
            _ => 0,
        };
        let plan = match plan {
            LogicalPlan::Limit(mut limit)
                if projections <= 1
                    && limit.skip.is_none()
                    && (matches!(
                        limit.fetch.as_deref(),
                        Some(Expr::Literal(
                            ScalarValue::Int32(Some(1)) | ScalarValue::Int64(Some(1)),
                            _
                        ))
                    ) || matches!(limit.fetch.as_deref(), Some(Expr::Cast(cast))
                        if cast.field.data_type() == &DataType::Int64
                            && matches!(cast.expr.as_ref(), Expr::Literal(ScalarValue::Int32(Some(1)), _)))) =>
            {
                let pruned =
                    self.prune_first_distinct(Arc::unwrap_or_clone(limit.input), 1 - projections)?;
                limit.input = Arc::new(pruned.data);
                if pruned.transformed {
                    // The consumed DISTINCT prepares its own descendants once.
                    return Ok(Transformed::yes(LogicalPlan::Limit(limit)));
                }
                LogicalPlan::Limit(limit)
            }
            plan => plan,
        };
        plan.map_children(|child| self.prepare_distinct_limits(child, projections))
    }

    fn correlated_exists_uses_inner_columns(&self, plan: &LogicalPlan) -> Result<bool> {
        plan.exists(|node| {
            let LogicalPlan::Filter(filter) = node else {
                return Ok(false);
            };
            let predicate = simplify_literal_boolean_predicate(filter.predicate.clone())?.data;
            Ok(split_conjunction_owned(predicate)
                .iter()
                .any(|expr| expr.contains_outer() && expr.any_column_refs()))
        })
    }

    fn prune_limit_zero(
        &self,
        node: LogicalPlan,
        retain_local_like: bool,
    ) -> Result<Transformed<LogicalPlan>> {
        if let LogicalPlan::Limit(limit) = &node
            && self.limit_count(limit.fetch.as_deref(), usize::MAX)? == Some(0)
        {
            // LIKE's local interpretation needs the actual query clock,
            // copied from the query state for the mandatory LIKE analyzer.
            // Retain only consumers eligible for early local conversion,
            // including all siblings.
            if retain_local_like && crate::like_errors::contains_early_local_like(&limit.input)? {
                return Ok(Transformed::no(node));
            }
            if known_empty(&limit.input) {
                return Ok(Transformed::yes(Arc::unwrap_or_clone(limit.input.clone())));
            }
            return Ok(Transformed::yes(LogicalPlan::EmptyRelation(
                datafusion_expr::logical_plan::EmptyRelation {
                    produce_one_row: false,
                    schema: Arc::clone(limit.input.schema()),
                },
            )));
        }
        Ok(Transformed::no(node))
    }

    // Keep input folds and their expression-derived declarations before converting local rows.
    fn simplify_local_input_expressions(
        &self,
        input: LogicalPlan,
    ) -> Result<Transformed<LogicalPlan>> {
        input.transform_down_up(
            |node| {
                let pruned = self.prune_limit_zero(node, true)?;
                let recursion = if pruned.transformed {
                    TreeNodeRecursion::Jump
                } else {
                    TreeNodeRecursion::Continue
                };
                Ok(Transformed::new(pruned.data, pruned.transformed, recursion))
            },
            |node| {
                if known_empty(&node) {
                    return Ok(Transformed::no(node));
                }
                let schema = Arc::new(datafusion_expr::utils::merge_schema(&node.inputs()));
                let info = datafusion_expr::simplify::SimplifyContext::builder()
                    .with_schema(schema)
                    .with_query_execution_start_time(self.props.query_execution_start_time)
                    .with_config_options(self.props.config_options.clone().unwrap_or_default())
                    .build();
                let simplifier = ExprSimplifier::new(info).with_local_input_errors();
                let names = datafusion_expr::expr_rewriter::NamePreserver::new(&node);
                let prepared = node.map_expressions(|expr| {
                    let name = names.save(&expr);
                    simplifier
                        .simplify_with_cycle_count_transformed(expr)
                        .map(|(prepared, _)| prepared.update_data(|expr| name.restore(expr)))
                })?;
                Ok(Transformed::new_transformed(
                    prepared.data.recompute_schema()?,
                    prepared.transformed,
                ))
            },
        )
    }

    fn cast_reachability(
        &self,
        plan: LogicalPlan,
        options: &ConfigOptions,
        exists: bool,
    ) -> Result<LogicalPlan> {
        use datafusion_expr::logical_plan::EmptyRelation;
        let is_spark_query = options
            .extensions
            .get::<SparkSqlExpressionSemantics>()
            .is_some();
        // Local VALUES conversion precedes pruning. Empty local inputs never
        // evaluate their projection; nonempty ones preserve live CAST failures.
        let plan = plan
            .transform_up(|mut node| {
                let replacement = if is_spark_query {
                    self.prepare_local_regexp_replacements(node, options)?
                } else {
                    Transformed::no(node)
                };
                node = replacement.data;
                let mut input_batch = None;
                let mut casts = false;
                let mut evaluable = true;
                let mut retain_input = false;
                let mut simplified_input = false;
                let mut literal_groups = false;
                if let LogicalPlan::Projection(project) = &mut node {
                    if is_spark_query {
                        literal_groups = project.expr.iter().try_fold(false, |found, expr| {
                            Ok::<_, datafusion_common::DataFusionError>(
                                found || local_literal_regexp_extract(expr)?,
                            )
                        })? && literal_regexp_input_permitted(&project.input)?;
                    }
                    for expr in &project.expr {
                        casts |= early_cast(expr, project.input.schema(), is_spark_query)?;
                        evaluable &= is_locally_evaluable(expr, true)?;
                    }
                    retain_input = is_spark_query
                        && self.spark_array_index_errors
                        && early_local_relation(&project.input)?
                        && input_permits_array_index_preparation(&project.input)?
                        && (literal_groups
                            || project.expr.iter().try_fold(false, |found, expr| {
                                Ok::<_, datafusion_common::DataFusionError>(
                                    found
                                        || expr.exists(|expr| {
                                            Ok(local_string_argument_bodies(expr)?.is_some()
                                                && array_index_error_candidate(expr)?)
                                        })?,
                                )
                            })?);
                    if retain_input {
                        let prepared = self.simplify_local_input_expressions(
                            Arc::unwrap_or_clone(Arc::clone(&project.input)),
                        )?;
                        simplified_input = prepared.transformed;
                        project.input = Arc::new(prepared.data);
                    }
                    if ((casts && (evaluable || is_spark_query)) || retain_input)
                        && !input_forbids_local_planning(&project.input)?
                    {
                        input_batch = self.local_batch(&project.input)?;
                    }
                    if let Some(batch) = &input_batch
                        && batch.num_rows() == 0
                    {
                        return Ok(Transformed::yes(LogicalPlan::EmptyRelation(
                            EmptyRelation {
                                produce_one_row: false,
                                schema: Arc::clone(&project.schema),
                            },
                        )));
                    }
                }
                let mut retained = false;
                if retain_input
                    && let Some(batch) = &input_batch
                    && let LogicalPlan::Projection(project) = &mut node
                {
                    let schema = Arc::clone(project.input.schema());
                    // Consume local conversion once; later previews and execution use inert values.
                    let input = if schema.fields().is_empty() {
                        // Values requires a column; an empty projection preserves only row count.
                        let carrier_schema = Arc::new(DFSchema::try_from(Schema::new(vec![
                            datafusion::arrow::datatypes::Field::new(
                                "__spark_input_row",
                                DataType::Int32,
                                false,
                            ),
                        ]))?);
                        let carrier = LogicalPlan::Values(datafusion_expr::Values {
                            schema: carrier_schema,
                            values: (0..batch.num_rows()).map(|_| vec![lit(0_i32)]).collect(),
                        });
                        LogicalPlan::Projection(
                            datafusion_expr::logical_plan::Projection::try_new_with_schema(
                                vec![],
                                Arc::new(carrier),
                                schema,
                            )?,
                        )
                    } else {
                        let values = (0..batch.num_rows())
                            .map(|row| {
                                batch
                                    .columns()
                                    .iter()
                                    .zip(schema.fields())
                                    .map(|(column, field)| {
                                        let value =
                                            ScalarValue::try_from_array(column.as_ref(), row)?;
                                        let metadata = (!field.metadata().is_empty())
                                            .then(|| FieldMetadata::from(field.as_ref()));
                                        Ok(Expr::Literal(value, metadata))
                                    })
                                    .collect::<Result<Vec<_>>>()
                            })
                            .collect::<Result<Vec<_>>>()?;
                        LogicalPlan::Values(datafusion_expr::Values { schema, values })
                    };
                    project.input = Arc::new(input);
                    retained = true;
                }
                let prepared = if is_spark_query && self.spark_array_index_errors {
                    self.prepare_array_errors_in_string_calls(node)?
                } else {
                    Transformed::no(node)
                };
                let node = prepared.data;
                if let LogicalPlan::Projection(project) = &node {
                    if is_spark_query && early_local_relation(&project.input)? {
                        for expr in &project.expr {
                            let mut expression = without_metadata_annotations(expr);
                            while let Expr::Alias(alias) = expression {
                                expression = without_metadata_annotations(&alias.expr);
                            }
                            let Expr::HigherOrderFunction(function) = expression else {
                                continue;
                            };
                            let Some(extract) = (function.func.inner().as_ref()
                                as &dyn std::any::Any)
                                .downcast_ref::<SparkRegexpExtract>()
                            else {
                                continue;
                            };
                            let empty = known_empty(&project.input);
                            let literal_group = literal_groups
                                && retained
                                && local_literal_regexp_extract(expression)?;
                            if !empty && !extract.requires_local_index_check() && !literal_group {
                                continue;
                            }
                            let Some([source, pattern, index]) =
                                SparkRegexpExtract::closed_argument_bodies(expression)?
                            else {
                                continue;
                            };
                            if literal_group {
                                // Input conversion is already retained as inert VALUES. Evaluate only
                                // the admitted index through its existing shared conversion kernel.
                                let value = match self.eval_constant(index)? {
                                    ColumnarValue::Scalar(value) => value,
                                    ColumnarValue::Array(array) if array.len() == 1 => {
                                        ScalarValue::try_from_array(array.as_ref(), 0)?
                                    }
                                    _ => return datafusion_common::internal_err!(
                                        "closed regexp index returned a result other than one row"
                                    ),
                                };
                                let info = datafusion_expr::simplify::SimplifyContext::builder()
                                    .with_schema(Arc::clone(project.input.schema()))
                                    .with_config_options(Arc::new(options.clone()))
                                    .build();
                                // The existing marker-gated simplifier checks matched groups and
                                // leaves parse/capability failures, no-match and success alone.
                                let _ = datafusion_expr::ScalarUDFImpl::simplify(
                                    extract,
                                    vec![
                                        lit(crate::function::common::literal_value_without_evaluation(source)
                                            .ok_or_else(|| datafusion_common::internal_datafusion_err!(
                                                "closed regexp source is not a literal"
                                            ))?),
                                        lit(crate::function::common::literal_value_without_evaluation(pattern)
                                            .ok_or_else(|| datafusion_common::internal_datafusion_err!(
                                                "closed regexp pattern is not a literal"
                                            ))?),
                                        lit(value),
                                    ],
                                    &info,
                                )?;
                                continue;
                            }
                            // Keep earlier local input work and NULL selection before index conversion.
                            let index = datafusion_expr::when(
                                source.clone().is_null().or(pattern.clone().is_null()),
                                lit(ScalarValue::Int64(None)),
                            )
                            .otherwise(index.clone())?;
                            if try_check_local_row_errors(&index, &project.input, &self.props)?
                                && empty
                            {
                                return Ok(Transformed::yes(LogicalPlan::EmptyRelation(
                                    EmptyRelation {
                                        produce_one_row: false,
                                        schema: Arc::clone(&project.schema),
                                    },
                                )));
                            }
                        }
                    }
                }
                if let LogicalPlan::Projection(project) = &node
                    && casts
                    && (evaluable || is_spark_query)
                    && let Some(batch) = &input_batch
                {
                    let info = datafusion_expr::simplify::SimplifyContext::builder()
                        .with_schema(Arc::clone(project.input.schema()))
                        .with_config_options(Arc::new(options.clone()))
                        .build();
                    let mut simplifier = ExprSimplifier::new(info);
                    if is_spark_query {
                        simplifier = simplifier.with_local_input_errors();
                    }
                    for expr in &project.expr {
                        if forbids_local_planning(expr)? || !is_locally_evaluable(expr, true)? {
                            continue;
                        }
                        // Keep local comparisons while lowering other UDFs normally.
                        let expr = expr
                            .clone()
                            .transform_up(|mut expr| {
                                if let Expr::ScalarFunction(f) = &mut expr
                                    && let Some(nullif) =
                                        f.func.inner().downcast_ref::<SparkNullIf>()
                                {
                                    f.func = Arc::new(datafusion_expr::ScalarUDF::from(
                                        nullif.for_local_values(),
                                    ));
                                    return Ok(Transformed::yes(expr));
                                }
                                Ok(Transformed::no(expr))
                            })?
                            .data;
                        let expr = materialize_metadata_literals(expr)?.data;
                        let expr = simplifier.simplify(expr)?;
                        create_physical_expr(&expr, project.input.schema(), &self.props)?
                            .evaluate(batch)?;
                    }
                }
                Ok(Transformed::new_transformed(
                    node,
                    replacement.transformed || prepared.transformed || retained || simplified_input,
                ))
            })?
            .data;
        let config = OptimizerContext::new_with_config_options(Arc::new(options.clone()));
        let plan = if exists {
            // Correlated EXISTS retains DISTINCT until its ordinary optimizer
            // pass, where its output can still require a failing expression.
            let input = if plan.exists(|node| Ok(node.contains_outer_reference()))? {
                plan
            } else {
                let pruned = self.prune_first_distinct(plan, 0)?;
                if pruned.transformed {
                    pruned.data
                } else {
                    self.prepare_distinct_limits(pruned.data, 1)?.data
                }
            };
            LogicalPlan::Projection(datafusion_expr::logical_plan::Projection::try_new(
                vec![lit(true)],
                Arc::new(input),
            )?)
        } else {
            self.prepare_distinct_limits(plan, 0)?.data
        };
        // Decide LIKE's early local eligibility before column pruning can
        // remove an unevaluable sibling. Other expressions retain their
        // existing post-projection LIMIT-zero pruning below.
        let plan = if is_spark_query {
            plan.transform_down(|node| {
                if let LogicalPlan::Limit(limit) = &node
                    && crate::like_errors::contains_spark_like(&limit.input)?
                    && !crate::like_errors::contains_early_local_like(&limit.input)?
                {
                    self.prune_limit_zero(node, false)
                } else {
                    Ok(Transformed::no(node))
                }
            })?
            .data
        } else {
            plan
        };
        let plan = self.prune_projections(plan, &config, exists)?;
        let plan = plan
            .transform_down(|node| self.prune_limit_zero(node, is_spark_query))?
            .data;
        let plan = if is_spark_query {
            plan.transform_up(lower_substrings_over_pruned_empty_input)?
                .data
        } else {
            plan
        };
        if !self.has_null_sensitive_comparison {
            return Ok(plan);
        }
        Ok(plan
            .transform_up(|node| {
                // Propagate nested LIMIT 0 before checking a parent's constants.
                let prepared = PropagateEmptyRelation::new().rewrite(node, &config)?;
                let node = prepared.data;
                node.apply_expressions(|expr| {
                    if expr.exists(|expr| {
                        Ok(matches!(expr, Expr::ScalarFunction(f) if f.func.inner().is::<SparkNullIf>()))
                    })? {
                        // Constant NULLIF comparisons precede empty filters, after LIMIT 0 pruning.
                        let schema = match &node {
                            LogicalPlan::TableScan(scan) => Arc::new(DFSchema::try_from_qualified_schema(
                                scan.table_name.clone(), &scan.source.schema(),
                            )?),
                            _ => Arc::new(datafusion_expr::utils::merge_schema(&node.inputs())),
                        };
                        let info = datafusion_expr::simplify::SimplifyContext::builder()
                            .with_schema(schema)
                            .with_config_options(Arc::new(options.clone()))
                            .build();
                        let prepared = ExprSimplifier::new(info).simplify(expr.clone())?;
                        self.check_expr(&prepared, true)?;
                    }
                    Ok(TreeNodeRecursion::Continue)
                })?;
                Ok(Transformed::new_transformed(node, prepared.transformed))
            })?
            .data)
    }

    fn prepare_nested_result_declarations(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        Ok(plan.transform_up_with_subqueries(|node| {
            let schema = match &node {
                LogicalPlan::TableScan(scan) => Arc::new(DFSchema::try_from_qualified_schema(
                    scan.table_name.clone(), &scan.source.schema(),
                )?),
                _ => Arc::new(datafusion_expr::utils::merge_schema(&node.inputs())),
            };
            let info = datafusion_expr::simplify::SimplifyContext::builder()
                .with_schema(schema)
                .with_config_options(self.props.config_options.clone().unwrap_or_default())
                .build();
            let names = datafusion_expr::expr_rewriter::NamePreserver::new(&node);
            node.map_expressions(|expr| {
                let name = names.save(&expr);
                Ok(expr.transform_down(|expr| {
                    let prepared = prune_coalesce_declaration_cast(expr, &info)?;
                    if prepared.transformed {
                        return Ok(prepared);
                    }
                    let expr = prepared.data;
                    if !matches!(&expr, Expr::ScalarFunction(f) if f.func.inner().is::<StructFunction>())
                        || !is_locally_evaluable(&expr, false)?
                        || expr.exists(|value| Ok(matches!(value, Expr::ScalarFunction(function)
                            if !function.func.inner().supports_constant_folding())))?
                    {
                        return Ok(Transformed::no(expr));
                    }
                    // Spark folds the whole constant STRUCT with its original child declarations.
                    let declared_type = expr.get_type(&DFSchema::empty())?;
                    let value = match self.eval_constant(&expr) {
                        Ok(ColumnarValue::Scalar(value)) => value.to_array()?,
                        Ok(ColumnarValue::Array(array)) if array.len() == 1 => array,
                        // Required errors and discarded branches keep the existing preparation order.
                        _ => return Ok(Transformed::no(expr)),
                    };
                    let value = arrow_cast(value.as_ref(), &declared_type)?;
                    Ok(Transformed::yes(lit(ScalarValue::try_from_array(value.as_ref(), 0)?)))
                })?.update_data(|expr| name.restore(expr)))
            })
        })?.data)
    }

    fn eval_constant(&self, expr: &Expr) -> Result<ColumnarValue> {
        let batch = RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(1)),
        )?;
        create_physical_expr(expr, &DFSchema::empty(), &self.props)?.evaluate(&batch)
    }

    fn check_constant_slice_parameters(&self, plan: &LogicalPlan) -> Result<()> {
        plan.apply_with_subqueries(|node| {
            node.apply_expressions(|expr| {
                if !expr.exists(|expr| {
                    Ok(matches!(expr, Expr::ScalarFunction(f) if f.func.inner().is::<SparkArraySlice>()))
                })?
                    || expr.contains_scalar_subquery()
                    || expr.exists(|expr| {
                        Ok(expr.is_volatile_node()
                            || matches!(expr, Expr::ScalarFunction(f)
                                if !f.func.inner().is::<SparkArraySlice>()
                                    && !f.func.inner().supports_constant_folding()))
                    })?
                {
                    return Ok(TreeNodeRecursion::Continue);
                }
                let schema = match node {
                    LogicalPlan::TableScan(scan) => Arc::new(DFSchema::try_from_qualified_schema(
                        scan.table_name.clone(),
                        &scan.source.schema(),
                    )?),
                    _ => Arc::new(datafusion_expr::utils::merge_schema(&node.inputs())),
                };
                let info = datafusion_expr::simplify::SimplifyContext::builder()
                    .with_schema(schema)
                    .with_config_options(self.props.config_options.clone().unwrap_or_default())
                    .build();
                let simplifier = ExprSimplifier::new(info);
                // Fold literal SLICE results before following conditional parameter errors.
                let checked = simplifier
                    .simplify(expr.clone())
                    .and_then(|prepared| self.check_expr(&prepared, true));
                // Other failures retain their existing preparation or execution stage.
                if let Err(error) = checked
                    && SparkArraySlice::is_parameter_error(&error)
                {
                    return Err(error);
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
            Ok(TreeNodeRecursion::Continue)
        })
        .map(|_| ())
    }

    fn limit_count(&self, expr: Option<&Expr>, default: usize) -> Result<Option<usize>> {
        let Some(expr) = expr else {
            return Ok(Some(default));
        };
        if !is_locally_evaluable(expr, false)? || !permits_constant_preparation(expr)? {
            return Ok(None);
        }
        // Before simplification, LIMIT/OFFSET can still be casts or arithmetic.
        // An unsupported bound is not an unbounded relation.
        Ok(match self.eval_constant(expr)? {
            ColumnarValue::Scalar(ScalarValue::Int32(Some(n))) => usize::try_from(n).ok(),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(n))) => usize::try_from(n).ok(),
            _ => None,
        })
    }

    // Lower native conditional functions without folding their live arithmetic.
    // Local relation conversion evaluates the divisor first; later constant
    // folding follows NULL propagation on either operand.
    fn precheck_expression(&self, expr: Expr, schema: &DFSchemaRef, local: bool) -> Result<Expr> {
        // Local VALUES must consume their original arithmetic/CAST operands
        // before the later NULL fold can discard a nested live failure.
        let expr = if local {
            expr
        } else {
            materialize_metadata_literals(expr)?.data
        };
        let info = datafusion_expr::simplify::SimplifyContext::builder()
            .with_schema(Arc::clone(schema))
            .with_config_options(Arc::clone(self.props.config_options.as_ref().unwrap()))
            .build();
        Ok(expr
            .transform_down(|expr| {
                let expr = if local {
                    let pruned = prune_null_divisor(expr, schema)?;
                    if pruned.transformed {
                        return Ok(pruned);
                    }
                    pruned.data
                } else if null_round(&expr) && !expr.contains_scalar_subquery() {
                    lit(ScalarValue::try_new_null(&expr.get_type(schema)?)?)
                } else {
                    prune_null(expr, &info)?.data
                };
                let expr = if let Expr::ScalarFunction(f) = &expr
                    && f.func.short_circuits()
                    // Local VALUES comparisons precede constant NULL propagation.
                    && !(local && f.func.inner().is::<SparkNullIf>())
                {
                    match f.func.simplify(f.args.clone(), &info)? {
                        datafusion_expr::simplify::ExprSimplifyResult::Simplified(lowered) => {
                            lowered
                        }
                        datafusion_expr::simplify::ExprSimplifyResult::Original(args) => {
                            f.func.call(args)
                        }
                    }
                } else {
                    expr
                };
                // Check the factory's nonconstant argument graph before CASE
                // pruning can hide required constants behind a computed NULL.
                if matches!(&expr, Expr::ScalarFunction(function)
                    if function.func.inner().downcast_ref::<SparkSubstring>()
                        .is_some_and(SparkSubstring::has_logical_arguments))
                {
                    return Ok(Transformed::new(expr, false, TreeNodeRecursion::Jump));
                }
                // Dead conditional parents can make a derived DIV column unused.
                // Do not simplify arithmetic or Boolean parents before checking it.
                if !local
                    && matches!(expr, Expr::Case(_))
                    && !preparation_error_candidate(&expr, schema, self.spark_array_index_errors)?
                    && let Ok(simplified) = ExprSimplifier::new(info.clone()).simplify(expr.clone())
                {
                    return Ok(Transformed::yes(simplified));
                }
                Ok(Transformed::no(expr))
            })?
            .data)
    }

    fn check_preparation_error_plan(
        &self,
        plan: &LogicalPlan,
        options: &ConfigOptions,
        limits_prepared: bool,
    ) -> Result<()> {
        // Local VALUES are evaluated before column pruning and LIMIT 0.
        // Successful constants need no extra copy, simplification or pruning.
        let requires_pruned_check = self.check_preparation_error_expressions(plan, true)?;
        if requires_pruned_check {
            let config = OptimizerContext::new_with_config_options(Arc::new(options.clone()));
            // NULL parents and unused columns disappear before constant folding,
            // but Boolean/filter simplification must not erase arithmetic failures.
            let pruned = plan
                .clone()
                .transform_up(|node| {
                    let schema = Arc::new(datafusion_expr::utils::merge_schema(&node.inputs()));
                    let names = datafusion_expr::expr_rewriter::NamePreserver::new(&node);
                    node.map_expressions(|expr| {
                        let name = names.save(&expr);
                        let expr = self.precheck_expression(expr, &schema, false)?;
                        Ok(Transformed::yes(name.restore(expr)))
                    })
                })?
                .data;
            let pruned = if limits_prepared {
                pruned
            } else {
                self.prepare_distinct_limits(pruned, 0)?.data
            };
            let pruned = self.prune_projections(pruned, &config, false)?;
            self.check_preparation_error_expressions(&pruned, false)?;
        }
        Ok(())
    }

    // True requests a second constant check after pruning; it is not an error verdict.
    fn check_preparation_error_expressions(
        &self,
        plan: &LogicalPlan,
        check_local_inputs: bool,
    ) -> Result<bool> {
        let mut needs_pruning = false;
        plan.apply(|node| {
            if !check_local_inputs
                && let LogicalPlan::Limit(limit) = node
                && self.limit_count(limit.fetch.as_deref(), usize::MAX)? == Some(0)
                && !known_empty(&limit.input)
            {
                return Ok(TreeNodeRecursion::Jump);
            }
            let schema = Arc::new(datafusion_expr::utils::merge_schema(&node.inputs()));
            node.apply_expressions(|expr| {
                let arithmetic = arithmetic_candidate(expr, &schema)?;
                let array_index =
                    self.spark_array_index_errors && array_index_error_candidate(expr)?;
                if !arithmetic && !array_index {
                    return Ok(TreeNodeRecursion::Continue);
                }
                let constant_preparation_permitted = permits_constant_preparation(expr)?;
                if !arithmetic
                    && array_index
                    && (!constant_preparation_permitted
                        || !node.inputs().iter().try_fold(true, |allowed, input| {
                            Ok::<_, datafusion_common::DataFusionError>(
                                allowed && input_permits_array_index_preparation(input)?,
                            )
                        })?)
                {
                    return Ok(TreeNodeRecursion::Continue);
                }
                let local_node =
                    matches!(node, LogicalPlan::Projection(_) | LogicalPlan::Filter(_));
                let batch = if local_node {
                    node.inputs()
                        .first()
                        .and_then(|input| self.local_batch(input).ok().flatten())
                } else {
                    None
                };
                // Empty local conversion consumes the expression without invoking it.
                let batch =
                    batch.filter(|batch| batch.num_rows() == 0 || constant_preparation_permitted);
                match batch {
                    Some(batch)
                        if check_local_inputs
                            && batch.num_rows() > 0
                            && is_locally_evaluable(expr, true)? =>
                    {
                        let expr = self.precheck_expression(expr.clone(), &schema, true)?;
                        let error = match create_physical_expr(&expr, &schema, &self.props)?
                            .evaluate(&batch)
                        {
                            Ok(_) => return Ok(TreeNodeRecursion::Continue),
                            Err(error) => error,
                        };
                        if !arithmetic {
                            if SparkArrayIndexError::is(&error) {
                                return Err(error);
                            }
                            return Ok(TreeNodeRecursion::Continue);
                        }
                        // Match Spark's strict string-to-integer trimming
                        // only for this discarded local evaluation. The
                        // runtime expression and its physical plan stay intact.
                        let expr = expr.clone().transform_up(|expr| {
                            let expr = if let Expr::Cast(mut cast) = expr {
                                if cast.field.data_type().is_signed_integer()
                                    && cast.expr.get_type(&schema)?.is_string()
                                {
                                    let trim: String = (0..=b' ')
                                        .chain(std::iter::once(0x7f))
                                        .map(char::from)
                                        .collect();
                                    cast.expr =
                                        Box::new(expr_fn::btrim(vec![*cast.expr, lit(trim)]));
                                    return Ok(Transformed::yes(Expr::Cast(cast)));
                                }
                                Expr::Cast(cast)
                            } else {
                                expr
                            };
                            let input = match &expr {
                                Expr::Cast(cast) if cast.field.data_type() == &DataType::Int64 => {
                                    Some(cast.expr.as_ref())
                                }
                                Expr::ScalarFunction(f)
                                    if f.func
                                        .inner()
                                        .downcast_ref::<SparkCastNumericToInteger>()
                                        .is_some_and(|cast| {
                                            !cast.is_try() && cast.target_type() == &DataType::Int64
                                        }) =>
                                {
                                    match f.args.as_slice() {
                                        [input] => Some(input),
                                        _ => None,
                                    }
                                }
                                _ => None,
                            };
                            // Spark rounds floating Long.MaxValue to 2^63,
                            // then saturates that endpoint in this local check.
                            if let Some(input) = input {
                                let upper = match input.get_type(&schema)? {
                                    DataType::Float32 => {
                                        Some(ScalarValue::Float32(Some(i64::MAX as f32)))
                                    }
                                    DataType::Float64 => {
                                        Some(ScalarValue::Float64(Some(i64::MAX as f64)))
                                    }
                                    _ => None,
                                };
                                if let Some(upper) = upper {
                                    let expr = datafusion_expr::expr_fn::when(
                                        input.clone().eq(lit(upper)),
                                        lit(i64::MAX),
                                    )
                                    .otherwise(expr)?;
                                    return Ok(Transformed::yes(expr));
                                }
                            }
                            Ok(Transformed::no(expr))
                        })?;
                        if !expr.transformed {
                            return Err(error);
                        }
                        create_physical_expr(&expr.data, &schema, &self.props)?.evaluate(&batch)?;
                    }
                    None => {
                        let expr = if !arithmetic
                            && array_index
                            && let LogicalPlan::Projection(project) = node
                        {
                            inline_closed_projection_bindings_for_array_check(
                                expr.clone(),
                                &project.input,
                            )?
                        } else {
                            expr.clone()
                        };
                        let expr = self.precheck_expression(expr, &schema, false)?;
                        let checked = match self.check_expr(&expr, true) {
                            Err(error) if arithmetic || SparkArrayIndexError::is(&error) => {
                                Err(error)
                            }
                            _ => Ok(()),
                        };
                        if check_local_inputs {
                            needs_pruning |= checked.is_err();
                        } else {
                            checked?;
                        }
                    }
                    // Local projection/filter conversion consumes even empty inputs.
                    _ => {}
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
            Ok(TreeNodeRecursion::Continue)
        })
        .map(|_| needs_pruning)
    }

    fn check_expr(&self, expr: &Expr, constants: bool) -> Result<()> {
        let expr = without_metadata_annotations(expr);
        if let Expr::ScalarSubquery(subquery) = expr {
            if let Some(expressions) = one_row_exprs(&subquery.subquery)? {
                return self.check_expr(&expressions[0], constants);
            }
            return self.check_subquery(&subquery.subquery);
        }
        // NULL propagation precedes constant folding. Only subquery planning
        // work survives the discarded branch, not its ordinary scalar errors.
        if null_discarded(expr) {
            return expr
                .apply_children(|child| {
                    self.check_expr(child, false)?;
                    Ok(TreeNodeRecursion::Continue)
                })
                .map(|_| ());
        }
        // SLICE opts out of generic evaluation but still validates reached literal parameters.
        if constants
            && let Expr::ScalarFunction(function) = expr
            && function.func.inner().is::<SparkArraySlice>()
            && let [
                Expr::Literal(array, _),
                Expr::Literal(ScalarValue::Int64(Some(start)), _),
                Expr::Literal(ScalarValue::Int64(Some(length)), _),
            ] = function.args.as_slice()
            && !array.is_null()
        {
            let _ = create_physical_expr(expr, &DFSchema::empty(), &self.props)?;
            SparkArraySlice::validate_parameters(*start, *length)?;
        }
        if constants && is_locally_evaluable(expr, false)? && permits_constant_preparation(expr)? {
            self.eval_constant(expr)?;
            return Ok(());
        }
        // An opaque parent still checks eager arguments; only conditional arguments defer errors.
        let conditional = match expr {
            Expr::ScalarFunction(function)
                if function.func.short_circuits()
                    && !function
                        .func
                        .inner()
                        .downcast_ref::<SparkSubstring>()
                        .is_some_and(SparkSubstring::has_logical_arguments) =>
            {
                if let Some((eager_arguments, conditional_arguments)) =
                    function.func.conditional_arguments(&function.args)
                {
                    for argument in eager_arguments {
                        self.check_expr(argument, constants)?;
                    }
                    for argument in conditional_arguments {
                        self.check_expr(argument, false)?;
                    }
                    return Ok(());
                }
                true
            }
            _ => matches!(expr, Expr::Case(_)) || local_string_argument_bodies(expr)?.is_some(),
        };
        expr.apply_children(|child| {
            self.check_expr(child, constants && !conditional)?;
            Ok(TreeNodeRecursion::Continue)
        })
        .map(|_| ())
    }

    fn check_subquery(&self, plan: &LogicalPlan) -> Result<()> {
        // ponytail: unsupported ancestors can revisit local inputs; use one
        // postorder pass if deeply nested discarded subqueries become costly.
        plan.apply(|node| {
            // A locally evaluated relation has replaced these expressions,
            // including projections over zero rows. Do not fold them again.
            if self.local_batch(node)?.is_some() {
                return Ok(TreeNodeRecursion::Jump);
            }
            node.apply_expressions(|expr| {
                self.check_expr(expr, true)?;
                Ok(TreeNodeRecursion::Continue)
            })?;
            Ok(TreeNodeRecursion::Continue)
        })
        .map(|_| ())
    }

    // Evaluate only SQL VALUES and the operations Spark can perform locally.
    // Table scans, range execution, aggregates and joins are never executed.
    // ARRAY-bearing string preparation retains admitted local values in the actual input plan.
    fn local_batch(&self, plan: &LogicalPlan) -> Result<Option<RecordBatch>> {
        let inputs = plan
            .inputs()
            .into_iter()
            .map(|p| self.local_batch(p))
            .collect::<Result<Vec<_>>>()?;
        let input = inputs.first().and_then(|p| p.as_ref());
        let batch = match plan {
            LogicalPlan::Values(values) if !values.values.is_empty() => {
                if !values.values.iter().flatten().try_fold(true, |valid, e| {
                    Ok::<_, datafusion_common::DataFusionError>(
                        valid
                            && is_locally_evaluable(e, false)?
                            && permits_constant_preparation(e)?,
                    )
                })? {
                    return Ok(None);
                }
                let exprs = values
                    .values
                    .iter()
                    .map(|row| {
                        row.iter()
                            .map(|e| create_physical_expr(e, &values.schema, &self.props))
                            .collect::<Result<Vec<_>>>()
                    })
                    .collect::<Result<Vec<_>>>()?;
                let source = MemorySourceConfig::try_new_as_values(
                    Arc::clone(values.schema.inner()),
                    exprs,
                )?;
                source
                    .data_source()
                    .downcast_ref::<MemorySourceConfig>()
                    .unwrap()
                    .partitions()[0][0]
                    .clone()
            }
            LogicalPlan::Projection(project) if input.is_some() => {
                let input = input.unwrap();
                if input.num_rows() == 0 {
                    return Ok(Some(RecordBatch::new_empty(Arc::clone(
                        project.schema.inner(),
                    ))));
                }
                if !project.expr.iter().try_fold(true, |valid, e| {
                    Ok::<_, datafusion_common::DataFusionError>(
                        valid && is_locally_evaluable(e, true)? && permits_constant_preparation(e)?,
                    )
                })? {
                    return Ok(None);
                }
                let columns = project
                    .expr
                    .iter()
                    .map(|e| {
                        let e =
                            self.precheck_expression(e.clone(), project.input.schema(), true)?;
                        create_physical_expr(&e, project.input.schema(), &self.props)?
                            .evaluate(input)?
                            .into_array(input.num_rows())
                    })
                    .collect::<Result<Vec<_>>>()?;
                RecordBatch::try_new_with_options(
                    Arc::clone(project.schema.inner()),
                    columns,
                    &RecordBatchOptions::new().with_row_count(Some(input.num_rows())),
                )?
            }
            LogicalPlan::SubqueryAlias(alias) if input.is_some() => {
                let input = input.unwrap();
                RecordBatch::try_new_with_options(
                    Arc::clone(alias.schema.inner()),
                    input.columns().to_vec(),
                    &RecordBatchOptions::new().with_row_count(Some(input.num_rows())),
                )?
            }
            LogicalPlan::Filter(filter) if input.is_some() => {
                let input = input.unwrap();
                if input.num_rows() == 0 {
                    return Ok(Some(input.clone()));
                }
                // Spark pulls correlated conjuncts out before recursively
                // optimizing this discarded subquery. Keep local predicates.
                let predicate = conjunction(
                    split_conjunction_owned(filter.predicate.clone())
                        .into_iter()
                        .filter(|e| !e.contains_outer()),
                )
                .unwrap_or_else(|| lit(true));
                if !is_locally_evaluable(&predicate, true)?
                    || !permits_constant_preparation(&predicate)?
                {
                    return Ok(None);
                }
                let predicate = self.precheck_expression(predicate, filter.input.schema(), true)?;
                let mask = create_physical_expr(&predicate, filter.input.schema(), &self.props)?
                    .evaluate(input)?
                    .into_array(input.num_rows())?;
                filter_record_batch(input, mask.as_boolean())?
            }
            LogicalPlan::Aggregate(aggregate)
                if input.is_some_and(|batch| batch.num_rows() == 0) =>
            {
                // Reuse native empty propagation; global/empty grouping-set
                // totals still produce rows. No aggregate is evaluated here.
                let mut aggregate = aggregate.clone();
                aggregate.input = Arc::new(LogicalPlan::EmptyRelation(
                    datafusion_expr::logical_plan::EmptyRelation {
                        produce_one_row: false,
                        schema: Arc::clone(aggregate.input.schema()),
                    },
                ));
                let config = OptimizerContext::new_with_config_options(Arc::clone(
                    self.props.config_options.as_ref().unwrap(),
                ));
                let prepared =
                    datafusion::optimizer::propagate_empty_relation::PropagateEmptyRelation::new()
                        .rewrite(LogicalPlan::Aggregate(aggregate), &config)?;
                if !matches!(prepared.data, LogicalPlan::EmptyRelation(empty) if !empty.produce_one_row)
                {
                    return Ok(None);
                }
                RecordBatch::new_empty(Arc::clone(plan.schema().inner()))
            }
            LogicalPlan::Sort(_) if input.is_some_and(|b| b.num_rows() == 0) => {
                input.unwrap().clone()
            }
            LogicalPlan::Limit(limit) if input.is_some() => {
                let input = input.unwrap();
                let Some(offset) = self.limit_count(limit.skip.as_deref(), 0)? else {
                    return Ok(None);
                };
                let Some(count) = self.limit_count(limit.fetch.as_deref(), usize::MAX)? else {
                    return Ok(None);
                };
                let offset = offset.min(input.num_rows());
                let count = count.min(input.num_rows() - offset);
                input.slice(offset, count)
            }
            _ => return Ok(None),
        };
        Ok(Some(batch))
    }
}

#[cfg(test)]
fn spark_test_frame(
    session: &datafusion::prelude::SessionContext,
    plan: LogicalPlan,
) -> datafusion::dataframe::DataFrame {
    let mut state = session.state();
    state
        .config_mut()
        .options_mut()
        .extensions
        .insert(SparkSqlExpressionSemantics::default());
    datafusion::dataframe::DataFrame::new(state, plan)
}

#[tokio::test]
async fn integer_divide_evaluation_order()
-> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use crate::{config::PlanConfig, physical_plan::SparkQueryPlanner, resolver::PlanResolver};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::SessionContext;

    let ctx = SessionContext::new_with_state(
        SessionStateBuilder::new()
            .with_default_features()
            .with_analyzer_rule(Arc::new(PrepareSparkExpressions))
            .with_query_planner(Arc::new(SparkQueryPlanner))
            .build(),
    );
    for data_type in ["TINYINT", "SMALLINT", "INT", "BIGINT"] {
        let div = format!("CAST(7 AS {data_type}) DIV CAST(0 AS {data_type})");
        for ansi in [true, false] {
            for (sql, fails, rows) in [
                (
                    format!("SELECT {div} AS q FROM range(3) WHERE false"),
                    true,
                    0,
                ),
                (format!("SELECT {div} AS q FROM range(3) LIMIT 0"), false, 0),
                (
                    format!("SELECT {div} AS q FROM range(3) WHERE false LIMIT 0"),
                    true,
                    0,
                ),
                (
                    format!(
                        "SELECT CASE WHEN true THEN {div} ELSE 5 END AS q FROM range(3) WHERE false"
                    ),
                    true,
                    0,
                ),
                (
                    format!(
                        "SELECT COALESCE(CAST(NULL AS BIGINT),{div}) AS q FROM range(3) WHERE false"
                    ),
                    true,
                    0,
                ),
                (
                    format!("SELECT ({div}>0) AND false AS q FROM range(3) WHERE false"),
                    true,
                    0,
                ),
                (
                    format!("SELECT false AND ({div}>0) AS q FROM range(3) WHERE false"),
                    false,
                    0,
                ),
                (
                    format!("SELECT id FROM range(3) WHERE false AND id>({div})"),
                    true,
                    0,
                ),
                (
                    format!("SELECT id FROM range(3) WHERE false ORDER BY {div}"),
                    true,
                    0,
                ),
                (
                    format!("SELECT SUM({div}) AS q FROM range(3) WHERE false"),
                    true,
                    1,
                ),
                (
                    format!(
                        "SELECT CASE WHEN k=0 THEN {div} ELSE 5 END AS q FROM VALUES (0) t(k) LIMIT 0"
                    ),
                    true,
                    0,
                ),
                (
                    format!(
                        "SELECT CASE WHEN k=1 THEN {div} ELSE 5 END AS q FROM VALUES (0) t(k) LIMIT 0"
                    ),
                    false,
                    0,
                ),
                (
                    format!(
                        "SELECT COALESCE(CAST(NULL AS BIGINT),{div}) AS q FROM VALUES (0) t(k) LIMIT 0"
                    ),
                    true,
                    0,
                ),
                (
                    format!(
                        "SELECT CASE WHEN false THEN q ELSE 5 END AS r FROM (SELECT {div} AS q FROM range(3)) t"
                    ),
                    false,
                    3,
                ),
                (
                    format!("SELECT COALESCE(5L,q) AS r FROM (SELECT {div} AS q FROM range(3)) t"),
                    false,
                    3,
                ),
                (
                    format!("SELECT {div} AS q FROM VALUES (0) t(id) LIMIT 0"),
                    true,
                    0,
                ),
                (
                    format!("SELECT CASE WHEN false THEN {div} ELSE 5 END AS q FROM range(3)"),
                    false,
                    3,
                ),
                (
                    format!(
                        "SELECT COALESCE(CAST(5 AS BIGINT), {div}) AS q FROM range(3) WHERE false"
                    ),
                    false,
                    0,
                ),
                (
                    format!(
                        "SELECT q * CAST(NULL AS BIGINT) AS r FROM (SELECT {div} AS q FROM range(3)) t"
                    ),
                    false,
                    3,
                ),
                (
                    format!("SELECT id FROM (SELECT id, {div} AS unused FROM range(3)) t"),
                    false,
                    3,
                ),
                (
                    format!("SELECT id FROM (SELECT id, {div} AS unused FROM VALUES (0) t(id)) t"),
                    true,
                    1,
                ),
            ] {
                let mut config = PlanConfig::default();
                config.ansi_mode = ansi;
                let ast = sail_sql_analyzer::parser::parse_one_statement(&sql)?;
                let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
                let named = PlanResolver::new(&ctx, Arc::new(config))
                    .resolve_named_plan(spec)
                    .await?;
                let result =
                    async { ctx.execute_logical_plan(named.plan).await?.collect().await }.await;
                if ansi && fails {
                    assert!(
                        result.unwrap_err().to_string().contains("Divide by zero"),
                        "{sql}"
                    );
                } else {
                    let batches = result?;
                    assert_eq!(
                        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
                        rows,
                        "{sql}, ANSI={ansi}"
                    );
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn null_integer_divide_checks_local_column_casts()
-> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use crate::{config::PlanConfig, physical_plan::SparkQueryPlanner, resolver::PlanResolver};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::SessionContext;

    let ctx = SessionContext::new_with_state(
        SessionStateBuilder::new()
            .with_default_features()
            .with_analyzer_rule(Arc::new(PrepareSparkExpressions))
            .with_query_planner(Arc::new(SparkQueryPlanner))
            .build(),
    );
    for data_type in ["TINYINT", "SMALLINT", "INT", "BIGINT"] {
        for ansi_mode in [true, false] {
            for (source, suffix, local_error, rows) in [
                ("VALUES ('bad') t(v)", "", true, 1),
                ("VALUES ('bad') t(v)", " LIMIT 0", true, 0),
                ("VALUES ('bad') t(v)", " WHERE false", false, 0),
                ("VALUES (' +2 ') t(v)", "", false, 1),
                (
                    "(SELECT v FROM VALUES ('bad') t(v) LIMIT 0) t",
                    "",
                    false,
                    0,
                ),
                (
                    "(SELECT v FROM VALUES ('2'), ('bad') t(v) LIMIT 1) t",
                    "",
                    false,
                    1,
                ),
                (
                    "(SELECT v FROM VALUES ('bad'), ('2') t(v) OFFSET 1) t",
                    "",
                    false,
                    1,
                ),
            ] {
                let sql = format!(
                    "SELECT CAST(NULL AS {data_type}) DIV CAST(v AS {data_type}) AS q FROM {source}{suffix}"
                );
                let ast = sail_sql_analyzer::parser::parse_one_statement(&sql)?;
                let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
                let config = PlanConfig {
                    ansi_mode,
                    ..Default::default()
                };
                let named = PlanResolver::new(&ctx, Arc::new(config))
                    .resolve_named_plan(spec)
                    .await?;
                let result =
                    async { ctx.execute_logical_plan(named.plan).await?.collect().await }.await;
                if ansi_mode && local_error {
                    let error = result.unwrap_err().to_string();
                    assert!(
                        error.contains("Cannot cast") && error.contains("bad"),
                        "{sql}: {error}"
                    );
                } else {
                    let batches = result?;
                    assert_eq!(
                        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
                        rows,
                        "{sql}"
                    );
                    assert!(
                        batches
                            .iter()
                            .all(|batch| batch.column(0).null_count() == batch.num_rows())
                    );
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn null_integer_divide_checks_numeric_casts()
-> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use crate::{config::PlanConfig, physical_plan::SparkQueryPlanner, resolver::PlanResolver};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::SessionContext;

    let ctx = SessionContext::new_with_state(
        SessionStateBuilder::new()
            .with_default_features()
            .with_analyzer_rule(Arc::new(PrepareSparkExpressions))
            .with_query_planner(Arc::new(SparkQueryPlanner))
            .build(),
    );
    for target in ["TINYINT", "SMALLINT", "INT", "BIGINT"] {
        for ansi_mode in [true, false] {
            for (value, fails) in [
                ("CAST('NaN' AS DOUBLE)", true),
                ("CAST('-Infinity' AS FLOAT)", true),
                (
                    "CAST(32768 AS BIGINT)",
                    matches!(target, "TINYINT" | "SMALLINT"),
                ),
                ("CAST('9223372036854775808' AS DOUBLE)", target != "BIGINT"),
                ("CAST('9223372036854775808' AS FLOAT)", target != "BIGINT"),
                ("CAST('9223372036854777856' AS DOUBLE)", true),
                ("CAST(2.75 AS DECIMAL(10,2))", false),
            ] {
                let sql = format!(
                    "SELECT CAST(NULL AS {target}) DIV CAST(v AS {target}) AS q FROM VALUES ({value}) t(v)"
                );
                let ast = sail_sql_analyzer::parser::parse_one_statement(&sql)?;
                let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
                let config = PlanConfig {
                    ansi_mode,
                    ..Default::default()
                };
                let named = PlanResolver::new(&ctx, Arc::new(config))
                    .resolve_named_plan(spec)
                    .await?;
                let result =
                    async { ctx.execute_logical_plan(named.plan).await?.collect().await }.await;
                if ansi_mode && fails {
                    let error = result.unwrap_err().to_string();
                    assert!(error.contains("Cast error"), "{sql}: {error}");
                } else {
                    let batches = result?;
                    assert_eq!(
                        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
                        1,
                        "{sql}"
                    );
                    assert!(
                        batches
                            .iter()
                            .all(|batch| batch.column(0).null_count() == batch.num_rows())
                    );
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn cast_reachability_preserves_early_local_errors()
-> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use crate::{config::PlanConfig, physical_plan::SparkQueryPlanner, resolver::PlanResolver};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::SessionContext;
    let ctx = SessionContext::new_with_state(
        SessionStateBuilder::new()
            .with_default_features()
            .with_analyzer_rule(Arc::new(PrepareSparkExpressions))
            .with_query_planner(Arc::new(SparkQueryPlanner))
            .build(),
    );
    for ansi in [true, false, true] {
        for (sql, ansi_error, expected) in [
            (
                "SELECT CASE WHEN true THEN CAST(NULL AS INT) DIV CAST(v AS INT) ELSE 5 END AS r FROM VALUES ('bad') t(v)",
                true,
                vec![None],
            ),
            (
                "SELECT CAST(NULL AS INT) DIV NULLIF(CAST(v AS INT),0) AS r FROM VALUES ('bad') t(v)",
                true,
                vec![None],
            ),
            (
                "SELECT CASE WHEN true THEN CAST(NULL AS INT) DIV CAST(v AS INT) ELSE 5 END AS r FROM VALUES (CAST('NaN' AS DOUBLE)) t(v)",
                true,
                vec![None],
            ),
            (
                "SELECT a / b AS r FROM VALUES (7D,0D) t(a,b) LIMIT 0",
                true,
                vec![],
            ),
            (
                "SELECT MOD(a,b) AS r FROM VALUES ('bad',2L) t(a,b) LIMIT 0",
                true,
                vec![],
            ),
            (
                "SELECT CAST(NULL AS DECIMAL(10,2)) / CAST('bad' AS DECIMAL(10,2)) AS r",
                false,
                vec![None],
            ),
            (
                "SELECT CAST('bad' AS DECIMAL(10,2)) DIV CAST(NULL AS DECIMAL(10,2)) AS r",
                false,
                vec![None],
            ),
            (
                "SELECT n + CAST(v AS DECIMAL(10,2)) AS r FROM VALUES ('bad',CAST(NULL AS DECIMAL(10,2))) t(v,n)",
                false,
                vec![None],
            ),
            (
                "SELECT CAST(v AS DECIMAL(10,2)) / n AS r FROM VALUES ('bad',CAST(NULL AS DECIMAL(10,2))),('7',2) t(v,n)",
                false,
                vec![None, Some(3.5)],
            ),
            (
                "SELECT CAST('7' AS DECIMAL(10,2)) DIV CAST(0 AS DECIMAL(10,2)) AS r",
                true,
                vec![None],
            ),
            (
                "SELECT SUM(CAST(a AS DECIMAL(38,2)) / CAST(b AS DECIMAL(38,2))) AS r FROM VALUES (0,'0.5','0.3'),(1,'0.5','0.5') AS t(id,a,b)",
                false,
                vec![Some(2.666667)],
            ),
            (
                "SELECT SUM(CAST(a AS DECIMAL(38,2)) / CAST(b AS DECIMAL(38,2))) OVER (ORDER BY id) AS r FROM VALUES (0,'0.5','0.3'),(1,'0.5','0.5') AS t(id,a,b) ORDER BY id",
                false,
                vec![Some(1.666667), Some(2.666667)],
            ),
            (
                "SELECT CAST('bad' AS DECIMAL(10,2)) AS r WHERE false",
                true,
                vec![],
            ),
            (
                "SELECT CAST('bad' AS DECIMAL(10,2)) AS r WHERE false LIMIT 0",
                true,
                vec![],
            ),
            (
                "SELECT CAST('bad' AS DECIMAL(10,2)) AS r FROM VALUES (0) t(k) LIMIT 0",
                true,
                vec![],
            ),
            (
                "SELECT 7 AS r FROM (SELECT CAST('bad' AS DECIMAL(10,2)) AS unused FROM VALUES (0) t(k)) t",
                true,
                vec![Some(7.0)],
            ),
            (
                "SELECT CAST('bad' AS DECIMAL(10,2)) AS r FROM range(3) WHERE false",
                true,
                vec![],
            ),
            (
                "SELECT CAST('bad' AS DECIMAL(10,2)) AS r FROM range(3) WHERE false LIMIT 0",
                true,
                vec![],
            ),
            (
                "SELECT CAST(NULL AS DOUBLE)/CAST('bad' AS DOUBLE) AS r",
                false,
                vec![None],
            ),
            (
                "SELECT CAST('bad' AS DOUBLE)/CAST(NULL AS DOUBLE) AS r",
                false,
                vec![None],
            ),
            (
                "SELECT CAST('bad' AS BIGINT) DIV CAST(NULL AS BIGINT) AS r",
                false,
                vec![None],
            ),
            (
                "SELECT CASE WHEN false THEN CAST('bad' AS INT) ELSE 7 END AS r",
                false,
                vec![Some(7.0)],
            ),
            (
                "SELECT COALESCE(7,CAST('bad' AS INT)) AS r",
                false,
                vec![Some(7.0)],
            ),
            (
                "SELECT CASE WHEN k=0 THEN 7D ELSE CAST('bad' AS DOUBLE) END AS r FROM VALUES (0),(0) t(k)",
                false,
                vec![Some(7.0), Some(7.0)],
            ),
            (
                "SELECT CAST(v AS BIGINT) DIV n AS r FROM VALUES ('bad',CAST(NULL AS BIGINT)),('7',2L) t(v,n)",
                false,
                vec![None, Some(3.0)],
            ),
            (
                "SELECT CAST('bad' AS INT) AS r FROM range(3) LIMIT 0",
                false,
                vec![],
            ),
            (
                "SELECT 7 AS r FROM (SELECT CAST('bad' AS INT) AS unused FROM range(3)) t",
                false,
                vec![Some(7.0); 3],
            ),
            (
                "SELECT CAST('bad' AS INT) AS r FROM VALUES (0) t(k) WHERE false",
                false,
                vec![],
            ),
            (
                "SELECT CAST('bad' AS INT) AS r FROM VALUES (0) t(k) LIMIT 0",
                true,
                vec![],
            ),
            (
                "SELECT 7 AS r FROM (SELECT CAST('bad' AS INT) AS unused FROM VALUES (0) t(k)) t",
                true,
                vec![Some(7.0)],
            ),
            (
                "SELECT CAST('bad' AS INT) AS r FROM range(3) WHERE false LIMIT 0",
                true,
                vec![],
            ),
            (
                "SELECT CAST(NULL AS BIGINT) DIV CAST(v AS BIGINT) AS r FROM VALUES ('bad') t(v)",
                true,
                vec![None],
            ),
            (
                "SELECT CAST(7 AS INT) DIV CAST(0 AS INT) AS r FROM VALUES (0) t(k) LIMIT 0",
                true,
                vec![],
            ),
        ] {
            let result = async {
                let ast = sail_sql_analyzer::parser::parse_one_statement(sql)?;
                let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
                let config = PlanConfig {
                    ansi_mode: ansi,
                    ..Default::default()
                };
                let named = PlanResolver::new(&ctx, Arc::new(config))
                    .resolve_named_plan(spec)
                    .await?;
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
                    ctx.execute_logical_plan(named.plan)
                        .await?
                        .collect()
                        .await?,
                )
            }
            .await;
            if ansi && ansi_error {
                assert!(result.is_err(), "{sql}");
                continue;
            }
            let mut actual = Vec::new();
            for batch in result? {
                for row in 0..batch.num_rows() {
                    let value = ScalarValue::try_from_array(batch.column(0), row)?
                        .cast_to(&DataType::Float64)?;
                    let ScalarValue::Float64(value) = value else {
                        unreachable!()
                    };
                    actual.push(value);
                }
            }
            assert_eq!(actual, expected, "ANSI {ansi}: {sql}");
        }
    }
    Ok(())
}

#[tokio::test]
async fn null_scale_retains_subquery_execution_without_dead_scalar_work()
-> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use crate::{config::PlanConfig, physical_plan::SparkQueryPlanner, resolver::PlanResolver};
    use datafusion::{execution::SessionStateBuilder, prelude::SessionContext};
    let ctx = SessionContext::new_with_state(
        SessionStateBuilder::new()
            .with_default_features()
            .with_analyzer_rule(Arc::new(PrepareSparkExpressions))
            .with_query_planner(Arc::new(SparkQueryPlanner))
            .build(),
    );
    for ansi_mode in [true, false, true] {
        for typ in ["INT", "BIGINT", "FLOAT", "DOUBLE", "DECIMAL(10,3)"] {
            for function in ["ROUND", "BROUND"] {
                for (sql, error, rows) in [
                    (
                        format!(
                            "SELECT {function}(CASE WHEN false THEN (SELECT CAST(v AS {typ}) FROM VALUES (2),(3) t(v)) ELSE CAST(1 AS {typ}) END,NULL) AS r"
                        ),
                        None,
                        1,
                    ),
                    (
                        format!(
                            "SELECT {function}(COALESCE(CAST(1 AS {typ}),(SELECT CAST(v AS {typ}) FROM VALUES (2),(3) t(v))),NULL) AS r"
                        ),
                        None,
                        1,
                    ),
                    (
                        format!(
                            "SELECT {function}(CASE WHEN true THEN CAST('bad' AS {typ}) ELSE (SELECT CAST(v AS {typ}) FROM VALUES (2),(3) t(v)) END,NULL) AS r"
                        ),
                        None,
                        1,
                    ),
                    (
                        format!(
                            "SELECT {function}(CAST((SELECT v FROM VALUES ('bad') t(v)) AS {typ}), NULL) AS r"
                        ),
                        None,
                        1,
                    ),
                    (
                        format!(
                            "SELECT {function}((SELECT CAST(v AS {typ}) FROM VALUES ('bad') t(v)), NULL) AS r"
                        ),
                        ansi_mode.then_some("Cannot cast"),
                        1,
                    ),
                    (
                        format!(
                            "SELECT {function}((SELECT CAST(v AS {typ}) FROM VALUES (2),(3) t(v)), NULL) AS r"
                        ),
                        Some("Scalar subquery returned more than one row"),
                        1,
                    ),
                    (
                        format!(
                            "SELECT {function}((SELECT CAST('bad' AS {typ}) FROM VALUES (0) t(k) WHERE false ORDER BY k), NULL) AS r"
                        ),
                        None,
                        1,
                    ),
                    (
                        format!("SELECT {function}((SELECT 1,2), NULL) AS r"),
                        Some("Scalar subquery must return exactly one column"),
                        1,
                    ),
                    (
                        format!(
                            "SELECT (SELECT CAST(id AS {typ}) FROM range(4) WHERE id>=o.k)/CAST(NULL AS {typ}) AS r FROM VALUES (1),(2) o(k) ORDER BY k"
                        ),
                        None,
                        2,
                    ),
                ] {
                    let result = async {
                        let ast = sail_sql_analyzer::parser::parse_one_statement(&sql)?;
                        let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
                        let config = PlanConfig {
                            ansi_mode,
                            ..Default::default()
                        };
                        let named = PlanResolver::new(&ctx, Arc::new(config))
                            .resolve_named_plan(spec)
                            .await?;
                        let data_type = named.plan.schema().field(0).data_type().clone();
                        let batches = ctx
                            .execute_logical_plan(named.plan)
                            .await?
                            .collect()
                            .await?;
                        Ok::<_, Box<dyn std::error::Error + Send + Sync>>((data_type, batches))
                    }
                    .await;
                    if let Some(error) = error {
                        let actual = result.unwrap_err().to_string();
                        assert!(actual.contains(error), "{sql}, ANSI={ansi_mode}: {actual}");
                    } else {
                        let (data_type, batches) =
                            result.map_err(|error| format!("{sql}, ANSI={ansi_mode}: {error}"))?;
                        assert_eq!(
                            batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
                            rows,
                            "{sql}"
                        );
                        assert!(
                            batches.iter().all(|b| b.column(0).data_type() == &data_type
                                && b.column(0).null_count() == b.num_rows()),
                            "{sql}"
                        );
                    }
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn scalar_subquery_arithmetic_precedes_outer_pruning()
-> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use crate::{config::PlanConfig, physical_plan::SparkQueryPlanner, resolver::PlanResolver};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::SessionContext;

    let ctx = SessionContext::new_with_state(
        SessionStateBuilder::new()
            .with_default_features()
            .with_analyzer_rule(Arc::new(PrepareSparkExpressions))
            .with_query_planner(Arc::new(SparkQueryPlanner))
            .build(),
    );
    for data_type in ["TINYINT", "SMALLINT", "INT", "BIGINT"] {
        let div = format!("CAST(7 AS {data_type}) DIV CAST(0 AS {data_type})");
        let sq = format!("(SELECT {div} FROM range(3) WHERE false)");
        for ansi_mode in [true, false] {
            for (sql, fails, rows, nulls) in [
                (
                    format!("SELECT CAST(NULL AS BIGINT) + (SELECT {div}) AS r"),
                    false,
                    1,
                    1,
                ),
                (
                    format!("SELECT CASE WHEN false THEN (SELECT {div}) ELSE 7L END AS r"),
                    false,
                    1,
                    0,
                ),
                (
                    format!("SELECT COALESCE(7L,(SELECT {div})) AS r"),
                    false,
                    1,
                    0,
                ),
                (format!("SELECT {sq} AS r"), true, 1, 1),
                (format!("SELECT (SELECT {sq}) AS r"), true, 1, 1),
                (
                    format!("SELECT (SELECT {div} FROM range(0)) AS r"),
                    true,
                    1,
                    1,
                ),
                (
                    format!("SELECT (SELECT {div} FROM range(3) LIMIT 0) AS r"),
                    false,
                    1,
                    1,
                ),
                (
                    format!("SELECT (SELECT {div} FROM range(3) WHERE false LIMIT 0) AS r"),
                    true,
                    1,
                    1,
                ),
                (
                    format!("SELECT (SELECT {div} FROM VALUES (1) t(k) WHERE false) AS r"),
                    false,
                    1,
                    1,
                ),
                (
                    format!("SELECT (SELECT {div} FROM VALUES (1) t(k) LIMIT 0) AS r"),
                    true,
                    1,
                    1,
                ),
                (
                    format!("SELECT CASE WHEN false THEN {sq} ELSE 7L END AS r"),
                    true,
                    1,
                    0,
                ),
                (format!("SELECT COALESCE(7L,{sq}) AS r"), true, 1, 0),
                (
                    format!("SELECT CAST(NULL AS BIGINT) + {sq} AS r"),
                    true,
                    1,
                    1,
                ),
                (format!("SELECT ROUND({sq},NULL) AS r"), true, 1, 1),
                (
                    format!("SELECT {sq} AS r FROM range(3) LIMIT 0"),
                    true,
                    0,
                    0,
                ),
                (
                    format!("SELECT id FROM (SELECT id,{sq} AS unused FROM range(2)) t"),
                    true,
                    2,
                    0,
                ),
                (
                    format!(
                        "SELECT (SELECT CASE WHEN false THEN {div} ELSE 7L END FROM range(1)) AS r"
                    ),
                    false,
                    1,
                    0,
                ),
                (
                    format!("SELECT (SELECT COALESCE(7L,{div}) FROM range(1)) AS r"),
                    false,
                    1,
                    0,
                ),
                (
                    format!(
                        "SELECT (SELECT id FROM (SELECT id,{div} AS unused FROM range(1)) t) AS r"
                    ),
                    false,
                    1,
                    0,
                ),
                (format!("SELECT EXISTS {sq} AS r"), false, 1, 0),
            ] {
                let ast = sail_sql_analyzer::parser::parse_one_statement(&sql)?;
                let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
                let named = PlanResolver::new(
                    &ctx,
                    Arc::new(PlanConfig {
                        ansi_mode,
                        ..Default::default()
                    }),
                )
                .resolve_named_plan(spec)
                .await?;
                let result = spark_test_frame(&ctx, named.plan).collect().await;
                if ansi_mode && fails {
                    assert!(
                        result.unwrap_err().to_string().contains("Divide by zero"),
                        "{sql}"
                    );
                } else {
                    let batches = result?;
                    assert_eq!(
                        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
                        rows,
                        "{sql}, ANSI={ansi_mode}"
                    );
                    assert_eq!(
                        batches
                            .iter()
                            .map(|b| b.column(0).null_count())
                            .sum::<usize>(),
                        nulls,
                        "{sql}, ANSI={ansi_mode}"
                    );
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn scalar_cast_preparation_respects_parent_and_empty_input()
-> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use crate::{config::PlanConfig, physical_plan::SparkQueryPlanner, resolver::PlanResolver};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::SessionContext;
    let ctx = SessionContext::new_with_state(
        SessionStateBuilder::new()
            .with_default_features()
            .with_analyzer_rule(Arc::new(PrepareSparkExpressions))
            .with_query_planner(Arc::new(SparkQueryPlanner))
            .build(),
    );
    for ansi in [true, false, true] {
        for (typ, divide) in [("INT", "DIV"), ("DOUBLE", "/"), ("DECIMAL(10,2)", "DIV")] {
            for (query, fails, expected) in [
                (
                    format!(
                        "SELECT CASE WHEN CAST(NULL AS BOOLEAN) THEN (SELECT CAST('bad' AS {typ})) ELSE CAST(7 AS {typ}) END AS r"
                    ),
                    false,
                    Some(7.0),
                ),
                (
                    format!(
                        "SELECT CASE WHEN false THEN (SELECT CAST('bad' AS {typ})) ELSE CAST(7 AS {typ}) END AS r"
                    ),
                    false,
                    Some(7.0),
                ),
                (
                    format!("SELECT (SELECT CAST('bad' AS {typ}) FROM range(3) LIMIT 0) AS r"),
                    false,
                    None,
                ),
                (
                    format!(
                        "SELECT (SELECT CAST('bad' AS {typ}) FROM VALUES (1) t(k) WHERE false) AS r"
                    ),
                    false,
                    None,
                ),
                (
                    format!("SELECT (SELECT CAST('bad' AS {typ})) AS r"),
                    true,
                    None,
                ),
                (
                    format!(
                        "SELECT (SELECT CAST('bad' AS {typ}) FROM VALUES (1) t(k) LIMIT 0) AS r"
                    ),
                    true,
                    None,
                ),
                (
                    format!("SELECT (SELECT CAST('bad' AS {typ}) FROM range(1)) AS r"),
                    true,
                    None,
                ),
                (
                    format!(
                        "SELECT COALESCE(CAST(7 AS {typ}), (SELECT CAST('bad' AS {typ}))) AS r"
                    ),
                    false,
                    Some(7.0),
                ),
                (
                    format!("SELECT ROUND((SELECT CAST('bad' AS {typ})), NULL) AS r"),
                    false,
                    None,
                ),
                (
                    format!(
                        "SELECT ROUND((SELECT CAST(7 AS {typ}) {divide} CAST(0 AS {typ})), NULL) AS r"
                    ),
                    false,
                    None,
                ),
                (
                    format!(
                        "SELECT BROUND((SELECT CAST(7 AS {typ}) {divide} CAST(0 AS {typ})), NULL) AS r"
                    ),
                    false,
                    None,
                ),
                (
                    format!(
                        "SELECT CAST(NULL AS {typ}) + (SELECT CAST('bad' AS {typ}) FROM range(3) LIMIT 0) AS r"
                    ),
                    false,
                    None,
                ),
                (
                    format!(
                        "SELECT CAST(NULL AS {typ}) + (SELECT CAST(v AS {typ}) FROM VALUES ('bad') t(v) LIMIT 0) AS r"
                    ),
                    true,
                    None,
                ),
            ] {
                let result = async {
                    let ast = sail_sql_analyzer::parser::parse_one_statement(&query)?;
                    let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
                    let config = PlanConfig {
                        ansi_mode: ansi,
                        ..Default::default()
                    };
                    let named = PlanResolver::new(&ctx, Arc::new(config))
                        .resolve_named_plan(spec)
                        .await?;
                    Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
                        ctx.execute_logical_plan(named.plan)
                            .await?
                            .collect()
                            .await?,
                    )
                }
                .await;
                if ansi && fails {
                    let error = result.expect_err(&query).to_string();
                    assert!(
                        error.contains("Cannot cast") || error.contains("CAST_INVALID_INPUT"),
                        "{query}: {error}"
                    );
                    continue;
                }
                let mut actual = Vec::new();
                for batch in result? {
                    for row in 0..batch.num_rows() {
                        let value = ScalarValue::try_from_array(batch.column(0), row)?
                            .cast_to(&DataType::Float64)?;
                        let ScalarValue::Float64(value) = value else {
                            unreachable!()
                        };
                        actual.push(value);
                    }
                }
                assert_eq!(actual, vec![expected], "ANSI {ansi}: {query}");
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn predicate_preparation_preserves_live_errors_and_unused_outputs()
-> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use crate::{config::PlanConfig, physical_plan::SparkQueryPlanner, resolver::PlanResolver};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::SessionContext;
    let ctx = SessionContext::new_with_state(
        SessionStateBuilder::new()
            .with_default_features()
            .with_analyzer_rule(Arc::new(PrepareSparkExpressions))
            .with_query_planner(Arc::new(SparkQueryPlanner))
            .build(),
    );
    for ansi in [true, false, true] {
        for (sql, error, expected) in [
            (
                "SELECT 7L IN (SELECT CAST(7 AS INT) DIV CAST(0 AS INT) FROM range(3) WHERE false) AS r",
                Some("Divide by zero"),
                Some(false),
            ),
            (
                "SELECT EXISTS(SELECT 7 DIV 0 FROM VALUES(1) t(k) LIMIT 0) AS r",
                Some("Divide by zero"),
                Some(false),
            ),
            (
                "SELECT 7 IN(SELECT 7 DIV 0 FROM VALUES(1) t(k) LIMIT 0) AS r",
                Some("Divide by zero"),
                Some(false),
            ),
            (
                "SELECT EXISTS(SELECT CAST('bad' AS INT) FROM range(1)) AS r",
                None,
                Some(true),
            ),
            (
                "SELECT NOT EXISTS(SELECT CAST('bad' AS INT) FROM range(1)) AS r",
                None,
                Some(false),
            ),
            (
                "SELECT EXISTS(SELECT CAST('bad' AS INT) FROM VALUES(1) t(k)) AS r",
                Some("Cannot cast"),
                Some(true),
            ),
            (
                "SELECT EXISTS(SELECT CAST(s AS INT) FROM VALUES('bad') t(s) LIMIT 0) AS r",
                Some("Cannot cast"),
                Some(false),
            ),
            (
                "SELECT EXISTS(SELECT 7 DIV 0 FROM range(3) WHERE false) AS r",
                None,
                Some(false),
            ),
            (
                "SELECT EXISTS(SELECT 7 DIV 0 FROM range(3) LIMIT 0) AS r",
                None,
                Some(false),
            ),
            (
                "SELECT 7 IN(SELECT 7 DIV 0 FROM range(3) LIMIT 0) AS r",
                None,
                Some(false),
            ),
            (
                "SELECT 7 IN(SELECT 7 DIV 0 FROM (SELECT id FROM range(3) LIMIT 0) t) AS r",
                None,
                Some(false),
            ),
            (
                "SELECT 7 IN(SELECT CAST('bad' AS INT) FROM VALUES(1) t(k) WHERE false) AS r",
                None,
                Some(false),
            ),
            (
                "SELECT NULL IN(SELECT id FROM range(3) LIMIT 0) AS r",
                None,
                Some(false),
            ),
            (
                "SELECT NULL NOT IN(SELECT id FROM range(3) LIMIT 0) AS r",
                None,
                Some(true),
            ),
            (
                "SELECT EXISTS(SELECT (SELECT CAST('bad' AS INT) FROM range(1)) FROM range(1)) AS r",
                Some("Cannot cast"),
                Some(true),
            ),
            (
                "SELECT CASE WHEN false THEN 7 IN(SELECT 7 DIV 0 FROM range(3) WHERE false) ELSE true END AS r",
                Some("Divide by zero"),
                Some(true),
            ),
            (
                "SELECT CASE WHEN false THEN EXISTS(SELECT 7 DIV 0 FROM range(3) WHERE false) ELSE true END AS r",
                None,
                Some(true),
            ),
            // The approved three-valued IN policy intentionally differs from Spark.
            (
                "SELECT 3 IN(SELECT k FROM VALUES(1),(CAST(NULL AS INT)) t(k)) AS r",
                None,
                None,
            ),
        ] {
            let result = async {
                let ast = sail_sql_analyzer::parser::parse_one_statement(sql)?;
                let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
                let config = PlanConfig {
                    ansi_mode: ansi,
                    ..Default::default()
                };
                let named = PlanResolver::new(&ctx, Arc::new(config))
                    .resolve_named_plan(spec)
                    .await?;
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
                    spark_test_frame(&ctx, named.plan).collect().await?,
                )
            }
            .await;
            if ansi && let Some(error) = error {
                let actual = result.expect_err(sql).to_string();
                assert!(actual.contains(error), "ANSI {ansi}: {sql}: {actual}");
                continue;
            }
            let mut actual = Vec::new();
            for batch in result? {
                for row in 0..batch.num_rows() {
                    let ScalarValue::Boolean(value) =
                        ScalarValue::try_from_array(batch.column(0), row)?
                    else {
                        panic!("{sql}: expected Boolean");
                    };
                    actual.push(value);
                }
            }
            assert_eq!(actual, vec![expected], "ANSI {ansi}: {sql}");
        }
    }
    Ok(())
}

#[tokio::test]
async fn exists_distinct_keeps_preparation_boundaries()
-> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use crate::{config::PlanConfig, physical_plan::SparkQueryPlanner, resolver::PlanResolver};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::SessionContext;
    let ctx = SessionContext::new_with_state(
        SessionStateBuilder::new()
            .with_default_features()
            .with_analyzer_rule(Arc::new(PrepareSparkExpressions))
            .with_query_planner(Arc::new(SparkQueryPlanner))
            .build(),
    );
    for ansi in [true, false, true] {
        for (sql, error, expected) in [
            (
                "SELECT EXISTS(SELECT DISTINCT CAST('bad' AS INT) FROM range(3)) AS r",
                None,
                Some(true),
            ),
            (
                "SELECT NOT EXISTS(SELECT DISTINCT CAST('bad' AS INT) AS v FROM range(3)) AS r",
                None,
                Some(false),
            ),
            (
                "SELECT EXISTS(SELECT DISTINCT CAST('bad' AS INT) AS v FROM VALUES(1) t(k)) AS r",
                Some("Cannot cast"),
                Some(true),
            ),
            (
                "SELECT EXISTS(SELECT DISTINCT CAST('bad' AS INT) AS v FROM VALUES(1) t(k) LIMIT 0) AS r",
                Some("Cannot cast"),
                Some(false),
            ),
            (
                "SELECT EXISTS(SELECT DISTINCT 7 DIV 0 AS v FROM VALUES(1) t(k) LIMIT 0) AS r",
                Some("Divide by zero"),
                Some(false),
            ),
            (
                "SELECT EXISTS(SELECT DISTINCT CAST('bad' AS INT) AS v FROM range(3) LIMIT 1) AS r",
                None,
                Some(true),
            ),
            (
                "SELECT EXISTS(SELECT DISTINCT CAST('bad' AS INT) AS v FROM range(3) LIMIT 2) AS r",
                Some("Cannot cast"),
                Some(true),
            ),
            (
                "SELECT EXISTS(SELECT DISTINCT CAST('bad' AS INT) AS v FROM range(3) OFFSET 0) AS r",
                Some("Cannot cast"),
                Some(true),
            ),
            (
                "SELECT EXISTS(SELECT DISTINCT v FROM (SELECT DISTINCT CAST('bad' AS INT) AS v FROM range(3)) t) AS r",
                Some("Cannot cast"),
                Some(true),
            ),
            (
                "SELECT EXISTS(SELECT v AS w FROM (SELECT DISTINCT CAST('bad' AS INT) AS v FROM range(3)) t) AS r",
                Some("Cannot cast"),
                Some(true),
            ),
            (
                "SELECT EXISTS(SELECT id FROM (SELECT DISTINCT id,CAST('bad' AS INT) AS v FROM range(3)) t) AS r",
                Some("Cannot cast"),
                Some(true),
            ),
            (
                "SELECT EXISTS(SELECT DISTINCT id%2 FROM range(3) OFFSET 2) AS r",
                None,
                Some(false),
            ),
            (
                "SELECT EXISTS(SELECT DISTINCT id%2 FROM range(3) OFFSET 1) AS r",
                None,
                Some(true),
            ),
            (
                "SELECT EXISTS(SELECT v FROM (SELECT DISTINCT CAST('bad' AS INT) AS v FROM range(3)) t) AS r",
                None,
                Some(true),
            ),
            (
                "SELECT EXISTS(SELECT v FROM (SELECT v FROM (SELECT DISTINCT CAST('bad' AS INT) AS v FROM range(3)) t) u) AS r",
                None,
                Some(true),
            ),
            (
                "SELECT EXISTS(SELECT DISTINCT CAST('bad' AS INT) FROM range(3) UNION ALL SELECT 7) AS r",
                Some("Cannot cast"),
                Some(true),
            ),
            (
                "SELECT EXISTS(SELECT CAST('bad' AS INT) FROM range(3) UNION SELECT 7) AS r",
                None,
                Some(true),
            ),
            (
                "SELECT EXISTS(SELECT DISTINCT (SELECT CAST('bad' AS INT) FROM range(1)) FROM range(3)) AS r",
                Some("Cannot cast"),
                Some(true),
            ),
            (
                "SELECT EXISTS(SELECT DISTINCT 7 FROM range(3) WHERE CAST('bad' AS INT)=id) AS r",
                Some("Cannot cast"),
                Some(false),
            ),
            (
                "SELECT EXISTS(SELECT DISTINCT CAST('bad' AS INT) FROM (SELECT id FROM range(3) LIMIT 0) t) AS r",
                None,
                Some(false),
            ),
            (
                "SELECT EXISTS(SELECT v FROM (SELECT DISTINCT CAST('bad' AS INT) AS v FROM range(3)) t WHERE v=7) AS r",
                Some("Cannot cast"),
                Some(false),
            ),
            (
                "SELECT EXISTS(SELECT DISTINCT CAST('bad' AS INT) AS v FROM range(3) WHERE false) AS r",
                None,
                Some(false),
            ),
        ] {
            let result = async {
                let ast = sail_sql_analyzer::parser::parse_one_statement(sql)?;
                let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
                let config = PlanConfig {
                    ansi_mode: ansi,
                    ..Default::default()
                };
                let named = PlanResolver::new(&ctx, Arc::new(config))
                    .resolve_named_plan(spec)
                    .await?;
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
                    spark_test_frame(&ctx, named.plan).collect().await?,
                )
            }
            .await;
            if ansi && let Some(error) = error {
                let actual = result.expect_err(sql).to_string();
                assert!(actual.contains(error), "ANSI {ansi}: {sql}: {actual}");
                continue;
            }
            let mut actual = Vec::new();
            for batch in result? {
                for row in 0..batch.num_rows() {
                    let ScalarValue::Boolean(value) =
                        ScalarValue::try_from_array(batch.column(0), row)?
                    else {
                        panic!("{sql}: expected Boolean");
                    };
                    actual.push(value);
                }
            }
            assert_eq!(actual, vec![expected], "ANSI {ansi}: {sql}");
        }
    }
    Ok(())
}

#[tokio::test]
async fn distinct_limit_keeps_preparation_boundaries()
-> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use crate::{config::PlanConfig, physical_plan::SparkQueryPlanner, resolver::PlanResolver};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::SessionContext;
    let ctx = SessionContext::new_with_state(
        SessionStateBuilder::new()
            .with_default_features()
            .with_analyzer_rule(Arc::new(PrepareSparkExpressions))
            .with_query_planner(Arc::new(SparkQueryPlanner))
            .build(),
    );
    for ansi in [true, false, true] {
        for (sql, error, expected) in [
            (
                "SELECT EXISTS(SELECT DISTINCT v FROM (SELECT DISTINCT CAST('bad' AS INT) AS v FROM range(3) LIMIT 1) t) AS r",
                None,
                ScalarValue::Boolean(Some(true)),
            ),
            (
                "SELECT NOT EXISTS(SELECT DISTINCT v FROM (SELECT DISTINCT CAST('bad' AS INT) AS v FROM range(3) LIMIT 1) t) AS r",
                None,
                ScalarValue::Boolean(Some(false)),
            ),
            (
                "SELECT EXISTS(SELECT DISTINCT v FROM (SELECT DISTINCT CAST('bad' AS INT) AS v FROM range(0) LIMIT 1) t) AS r",
                None,
                ScalarValue::Boolean(Some(false)),
            ),
            (
                "SELECT EXISTS(SELECT DISTINCT v FROM (SELECT DISTINCT CAST('bad' AS INT) AS v FROM VALUES(1),(2) t(k) LIMIT 1) t) AS r",
                Some("Cannot cast"),
                ScalarValue::Boolean(Some(true)),
            ),
            (
                "SELECT EXISTS(SELECT DISTINCT v FROM (SELECT DISTINCT CAST('bad' AS INT) AS v FROM VALUES(1),(2) t(k) WHERE false LIMIT 1) t) AS r",
                None,
                ScalarValue::Boolean(Some(false)),
            ),
            (
                "SELECT (SELECT 7 FROM (SELECT DISTINCT CAST('bad' AS INT) AS v FROM range(3) LIMIT 1) t) AS r",
                None,
                ScalarValue::Int32(Some(7)),
            ),
            (
                "SELECT 7 IN(SELECT 7 FROM (SELECT DISTINCT CAST('bad' AS INT) AS v FROM range(3) LIMIT 1) t) AS r",
                None,
                ScalarValue::Boolean(Some(true)),
            ),
            (
                "SELECT 7 AS r FROM (SELECT DISTINCT CAST('bad' AS INT) AS v FROM range(3) LIMIT 1) t",
                None,
                ScalarValue::Int32(Some(7)),
            ),
            (
                "SELECT count(*) AS r FROM (SELECT DISTINCT CAST('bad' AS INT) AS v FROM range(3) LIMIT 1) t",
                None,
                ScalarValue::Int64(Some(1)),
            ),
            (
                "SELECT (SELECT 7 FROM (SELECT DISTINCT 7 DIV 0 AS v FROM range(3) LIMIT 1) t) AS r",
                None,
                ScalarValue::Int32(Some(7)),
            ),
            (
                "SELECT 7 AS r FROM (SELECT DISTINCT 7 DIV 0 AS v FROM range(3) LIMIT 1) t",
                None,
                ScalarValue::Int32(Some(7)),
            ),
            (
                "SELECT v IS NULL AS r FROM (SELECT DISTINCT CAST('bad' AS INT) AS v FROM range(3) LIMIT 1) t",
                Some("Cannot cast"),
                ScalarValue::Boolean(Some(true)),
            ),
            (
                "SELECT EXISTS(SELECT v FROM (SELECT DISTINCT CAST('bad' AS INT) AS v FROM range(3) LIMIT 1) t WHERE v IS NULL) AS r",
                Some("Cannot cast"),
                ScalarValue::Boolean(Some(true)),
            ),
            (
                "SELECT EXISTS(SELECT DISTINCT v FROM (SELECT DISTINCT CAST('bad' AS INT) AS v FROM range(3) LIMIT 2) t) AS r",
                Some("Cannot cast"),
                ScalarValue::Boolean(Some(true)),
            ),
            (
                "SELECT EXISTS(SELECT DISTINCT v FROM (SELECT DISTINCT CAST('bad' AS INT) AS v FROM range(3) LIMIT 1 OFFSET 0) t) AS r",
                Some("Cannot cast"),
                ScalarValue::Boolean(Some(true)),
            ),
            (
                "SELECT EXISTS(SELECT DISTINCT v FROM (SELECT DISTINCT CAST('bad' AS INT) AS v FROM range(3) ORDER BY v LIMIT 1) t) AS r",
                Some("Cannot cast"),
                ScalarValue::Boolean(Some(true)),
            ),
            (
                "SELECT count(*) AS r FROM (SELECT DISTINCT CAST(id%2 AS INT) AS v FROM range(5) LIMIT 1 OFFSET 2) t",
                None,
                ScalarValue::Int64(Some(0)),
            ),
            (
                "SELECT count(*) AS r FROM (SELECT DISTINCT CAST(id%2 AS INT) AS v FROM range(5) LIMIT 1 OFFSET 1) t",
                None,
                ScalarValue::Int64(Some(1)),
            ),
            (
                "SELECT EXISTS(SELECT DISTINCT v FROM (SELECT DISTINCT CAST('bad' AS INT) AS v FROM range(3)) t LIMIT 1) AS r",
                Some("Cannot cast"),
                ScalarValue::Boolean(Some(true)),
            ),
            (
                "SELECT CASE WHEN false THEN EXISTS(SELECT DISTINCT v FROM (SELECT DISTINCT CAST('bad' AS INT) AS v FROM range(3)) t LIMIT 1) ELSE true END AS r",
                Some("Cannot cast"),
                ScalarValue::Boolean(Some(true)),
            ),
            (
                "SELECT EXISTS(SELECT DISTINCT v FROM (SELECT DISTINCT 7 DIV 0 AS v FROM range(3)) t LIMIT 1) AS r",
                Some("Divide by zero"),
                ScalarValue::Boolean(Some(true)),
            ),
            (
                "SELECT CASE WHEN false THEN EXISTS(SELECT DISTINCT v FROM (SELECT DISTINCT 7 DIV 0 AS v FROM range(3)) t LIMIT 1) ELSE true END AS r",
                Some("Divide by zero"),
                ScalarValue::Boolean(Some(true)),
            ),
            (
                "SELECT CASE WHEN false THEN EXISTS(SELECT v FROM (SELECT DISTINCT v FROM (SELECT DISTINCT 7 DIV 0 AS v FROM range(3)) t LIMIT 1) u LIMIT 1) ELSE true END AS r",
                Some("Divide by zero"),
                ScalarValue::Boolean(Some(true)),
            ),
            (
                "SELECT EXISTS(SELECT DISTINCT v FROM (SELECT DISTINCT CAST('bad' AS INT) AS v FROM range(3) LIMIT 2-1) t) AS r",
                Some("Cannot cast"),
                ScalarValue::Boolean(Some(true)),
            ),
            (
                "SELECT EXISTS(SELECT DISTINCT v FROM (SELECT DISTINCT CAST('bad' AS INT) AS v FROM range(3) LIMIT CAST(1 AS INT)) t) AS r",
                Some("Cannot cast"),
                ScalarValue::Boolean(Some(true)),
            ),
            (
                "SELECT EXISTS(SELECT DISTINCT v FROM (SELECT DISTINCT (SELECT CAST('bad' AS INT) FROM range(1)) AS v FROM range(3) LIMIT 1) t) AS r",
                Some("Cannot cast"),
                ScalarValue::Boolean(Some(true)),
            ),
        ] {
            let result = async {
                let ast = sail_sql_analyzer::parser::parse_one_statement(sql)?;
                let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
                let config = PlanConfig {
                    ansi_mode: ansi,
                    ..Default::default()
                };
                let named = PlanResolver::new(&ctx, Arc::new(config))
                    .resolve_named_plan(spec)
                    .await?;
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
                    spark_test_frame(&ctx, named.plan).collect().await?,
                )
            }
            .await;
            if ansi && let Some(error) = error {
                let actual = result.expect_err(sql).to_string();
                assert!(actual.contains(error), "ANSI {ansi}: {sql}: {actual}");
                continue;
            }
            let mut actual = Vec::new();
            for batch in result? {
                for row in 0..batch.num_rows() {
                    let value = ScalarValue::try_from_array(batch.column(0), row)?;
                    actual.push(value);
                }
            }
            assert_eq!(actual, vec![expected], "ANSI {ansi}: {sql}");
        }
    }
    Ok(())
}

#[tokio::test]
async fn projection_limit_keeps_preparation_boundaries()
-> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use crate::{config::PlanConfig, physical_plan::SparkQueryPlanner, resolver::PlanResolver};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::SessionContext;
    let ctx = SessionContext::new_with_state(
        SessionStateBuilder::new()
            .with_default_features()
            .with_analyzer_rule(Arc::new(PrepareSparkExpressions))
            .with_query_planner(Arc::new(SparkQueryPlanner))
            .build(),
    );
    for ansi in [true, false, true] {
        for (sql, strict, legacy) in [
            (
                "SELECT EXISTS(SELECT DISTINCT w FROM (SELECT v AS w FROM (SELECT DISTINCT CAST('bad' AS INT) AS v FROM range(3)) t LIMIT 1) u) AS r",
                Ok(vec![ScalarValue::Boolean(Some(true))]),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT EXISTS(SELECT DISTINCT v FROM (SELECT 7 AS v FROM (SELECT DISTINCT CAST('bad' AS INT) AS v FROM range(3)) t LIMIT 1) u) AS r",
                Ok(vec![ScalarValue::Boolean(Some(true))]),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT EXISTS(SELECT DISTINCT w FROM (SELECT v AS w FROM (SELECT DISTINCT CAST('bad' AS DOUBLE) AS v FROM range(3)) t LIMIT 1) u) AS r",
                Ok(vec![ScalarValue::Boolean(Some(true))]),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT EXISTS(SELECT DISTINCT v FROM (SELECT 7 AS v FROM (SELECT DISTINCT CAST('bad' AS DOUBLE) AS v FROM range(3)) t LIMIT 1) u) AS r",
                Ok(vec![ScalarValue::Boolean(Some(true))]),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT EXISTS(SELECT DISTINCT w FROM (SELECT v AS w FROM (SELECT DISTINCT CAST('bad' AS DECIMAL(10,2)) AS v FROM range(3)) t LIMIT 1) u) AS r",
                Ok(vec![ScalarValue::Boolean(Some(true))]),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT EXISTS(SELECT DISTINCT v FROM (SELECT 7 AS v FROM (SELECT DISTINCT CAST('bad' AS DECIMAL(10,2)) AS v FROM range(3)) t LIMIT 1) u) AS r",
                Ok(vec![ScalarValue::Boolean(Some(true))]),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT EXISTS(SELECT DISTINCT w FROM (SELECT v AS w FROM (SELECT DISTINCT 7 DIV 0 AS v FROM range(3)) t LIMIT 1) u) AS r",
                Ok(vec![ScalarValue::Boolean(Some(true))]),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT EXISTS(SELECT DISTINCT v FROM (SELECT 7 AS v FROM (SELECT DISTINCT 7 DIV 0 AS v FROM range(3)) t LIMIT 1) u) AS r",
                Ok(vec![ScalarValue::Boolean(Some(true))]),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT EXISTS(SELECT v AS w0 FROM (SELECT DISTINCT CAST('bad' AS INT) AS v FROM range(3)) t LIMIT 1) AS r",
                Err("Cannot cast"),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT EXISTS(SELECT DISTINCT w1 FROM (SELECT w0 AS w1 FROM (SELECT v AS w0 FROM (SELECT DISTINCT CAST('bad' AS INT) AS v FROM range(3)) t) t LIMIT 1) u) AS r",
                Err("Cannot cast"),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT count(*) AS r FROM (SELECT 7 AS w0 FROM (SELECT DISTINCT CAST('bad' AS INT) AS v FROM range(3)) t LIMIT 1) u",
                Ok(vec![ScalarValue::Int64(Some(1))]),
                Ok(vec![ScalarValue::Int64(Some(1))]),
            ),
            (
                "SELECT count(*) AS r FROM (SELECT 7 AS w1 FROM (SELECT 7 AS w0 FROM (SELECT DISTINCT CAST('bad' AS INT) AS v FROM range(3)) t) t LIMIT 1) u",
                Err("Cannot cast"),
                Ok(vec![ScalarValue::Int64(Some(1))]),
            ),
            (
                "SELECT EXISTS(SELECT DISTINCT w0 FROM (SELECT v+1 AS w0 FROM (SELECT DISTINCT CAST('bad' AS INT) AS v FROM range(3)) t LIMIT 1) u) AS r",
                Ok(vec![ScalarValue::Boolean(Some(true))]),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT EXISTS(SELECT DISTINCT v FROM (SELECT v FROM (SELECT v FROM (SELECT v FROM (SELECT DISTINCT CAST('bad' AS INT) AS v FROM range(3)) t) t) t LIMIT 1) u) AS r",
                Ok(vec![ScalarValue::Boolean(Some(true))]),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT EXISTS(SELECT DISTINCT w FROM (SELECT v AS w FROM (SELECT DISTINCT CAST('bad' AS INT) AS v, id AS k FROM VALUES(0),(1) t(id)) t LIMIT 1) u) AS r",
                Err("Cannot cast"),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT EXISTS(SELECT DISTINCT w FROM (SELECT v AS w FROM (SELECT DISTINCT CAST('bad' AS INT) AS v, id AS k FROM VALUES(0),(1) t(id) WHERE false) t LIMIT 1) u) AS r",
                Ok(vec![ScalarValue::Boolean(Some(false))]),
                Ok(vec![ScalarValue::Boolean(Some(false))]),
            ),
            (
                "SELECT w IS NULL AS r FROM (SELECT v AS w FROM (SELECT DISTINCT CAST('bad' AS INT) AS v, id AS k FROM range(3)) t LIMIT 1) u",
                Err("Cannot cast"),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT EXISTS(SELECT w FROM (SELECT v AS w FROM (SELECT DISTINCT CAST('bad' AS INT) AS v, id AS k FROM range(3)) t LIMIT 1) u WHERE w IS NULL) AS r",
                Err("Cannot cast"),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT EXISTS(SELECT DISTINCT w FROM (SELECT v AS w FROM (SELECT DISTINCT CAST('bad' AS INT) AS v FROM range(3)) t LIMIT 2-1) u) AS r",
                Err("Cannot cast"),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT EXISTS(SELECT DISTINCT w FROM (SELECT v AS w FROM (SELECT DISTINCT CAST('bad' AS INT) AS v FROM range(3)) t LIMIT CAST(1 AS INT)) u) AS r",
                Err("Cannot cast"),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT EXISTS(SELECT DISTINCT w FROM (SELECT v AS w FROM (SELECT DISTINCT CAST('bad' AS INT) AS v FROM range(3)) t ORDER BY w LIMIT 1) u) AS r",
                Err("Cannot cast"),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT EXISTS(SELECT DISTINCT w FROM (SELECT v AS w FROM (SELECT DISTINCT CAST('bad' AS INT) AS v FROM range(3) ORDER BY v) t LIMIT 1) u) AS r",
                Err("Cannot cast"),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT EXISTS(SELECT DISTINCT w FROM (SELECT v AS w FROM (SELECT DISTINCT CAST('bad' AS INT) AS v FROM range(3)) t LIMIT 1 OFFSET 0) u) AS r",
                Err("Cannot cast"),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT EXISTS(SELECT DISTINCT w FROM (SELECT v AS w FROM (SELECT DISTINCT CAST('bad' AS INT) AS v FROM range(3)) t LIMIT 1 OFFSET 1) u) AS r",
                Err("Cannot cast"),
                Ok(vec![ScalarValue::Boolean(Some(false))]),
            ),
            (
                "SELECT EXISTS(SELECT DISTINCT w FROM (SELECT v AS w FROM (SELECT DISTINCT CAST('bad' AS INT) AS v FROM range(3)) t LIMIT 0) u) AS r",
                Ok(vec![ScalarValue::Boolean(Some(false))]),
                Ok(vec![ScalarValue::Boolean(Some(false))]),
            ),
            (
                "SELECT EXISTS(SELECT DISTINCT w FROM (SELECT v AS w FROM (SELECT DISTINCT CAST('bad' AS INT) AS v FROM range(3)) t LIMIT 2) u) AS r",
                Err("Cannot cast"),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT EXISTS(SELECT DISTINCT w FROM (SELECT v AS w FROM (SELECT DISTINCT v FROM (SELECT DISTINCT CAST('bad' AS INT) AS v FROM range(3)) t) u LIMIT 1) u) AS r",
                Err("Cannot cast"),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT EXISTS(SELECT DISTINCT w FROM (SELECT w FROM (SELECT v AS w FROM (SELECT DISTINCT CAST('bad' AS INT) AS v FROM range(3)) t LIMIT 1) u LIMIT 1) u) AS r",
                Ok(vec![ScalarValue::Boolean(Some(true))]),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT EXISTS(SELECT DISTINCT w FROM (SELECT v AS w FROM (SELECT DISTINCT (SELECT CAST('bad' AS INT) FROM range(1)) AS v FROM range(3)) t LIMIT 1) u) AS r",
                Err("Cannot cast"),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT w IS NULL AS r FROM (SELECT v AS w FROM (SELECT DISTINCT 7 DIV 0 AS v, id AS k FROM range(3)) t LIMIT 1) u",
                Err("Divide by zero"),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT EXISTS(SELECT DISTINCT w FROM (SELECT v AS w FROM (SELECT DISTINCT 7 DIV 0 AS v, id AS k FROM VALUES(0),(1) t(id)) t LIMIT 1) u) AS r",
                Err("Divide by zero"),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT count(*) AS r FROM (SELECT v AS w FROM (SELECT DISTINCT CAST(id%2 AS INT) AS v FROM range(5)) t LIMIT 1 OFFSET 2) u",
                Ok(vec![ScalarValue::Int64(Some(0))]),
                Ok(vec![ScalarValue::Int64(Some(0))]),
            ),
            (
                "SELECT count(*) AS r FROM (SELECT v AS w FROM (SELECT DISTINCT CAST(id%2 AS INT) AS v FROM range(5)) t LIMIT 1 OFFSET 1) u",
                Ok(vec![ScalarValue::Int64(Some(1))]),
                Ok(vec![ScalarValue::Int64(Some(1))]),
            ),
            (
                "SELECT 7 AS r FROM (SELECT DISTINCT CAST('bad' AS INT) AS v FROM range(3)) t LIMIT 1",
                Ok(vec![ScalarValue::Int32(Some(7))]),
                Ok(vec![ScalarValue::Int32(Some(7))]),
            ),
        ] {
            let result = async {
                let ast = sail_sql_analyzer::parser::parse_one_statement(sql)?;
                let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
                let config = PlanConfig {
                    ansi_mode: ansi,
                    ..Default::default()
                };
                let named = PlanResolver::new(&ctx, Arc::new(config))
                    .resolve_named_plan(spec)
                    .await?;
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
                    spark_test_frame(&ctx, named.plan).collect().await?,
                )
            }
            .await;
            match if ansi { strict } else { legacy } {
                Err(fragment) => {
                    let error = result.expect_err(sql).to_string();
                    assert!(error.contains(fragment), "ANSI {ansi}: {sql}: {error}");
                }
                Ok(expected) => {
                    let mut actual = Vec::new();
                    for batch in result? {
                        for row in 0..batch.num_rows() {
                            actual.push(ScalarValue::try_from_array(batch.column(0), row)?);
                        }
                    }
                    assert_eq!(actual, expected, "ANSI {ansi}: {sql}");
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn foldable_grouping_keeps_empty_and_live_boundaries()
-> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use crate::{config::PlanConfig, physical_plan::SparkQueryPlanner, resolver::PlanResolver};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::SessionContext;
    let ctx = SessionContext::new_with_state(
        SessionStateBuilder::new()
            .with_default_features()
            .with_analyzer_rule(Arc::new(PrepareSparkExpressions))
            .with_query_planner(Arc::new(SparkQueryPlanner))
            .build(),
    );
    for ansi in [true, false, true] {
        for (sql, strict, legacy) in [
            (
                "SELECT EXISTS(SELECT CAST('bad' AS INT) AS v FROM range(3) GROUP BY CAST('bad' AS INT)) AS r",
                Ok(vec![ScalarValue::Boolean(Some(true))]),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT count(*) AS r FROM (SELECT CAST('bad' AS INT) AS v FROM range(3) GROUP BY CAST('bad' AS INT)) u",
                Ok(vec![ScalarValue::Int64(Some(1))]),
                Ok(vec![ScalarValue::Int64(Some(1))]),
            ),
            (
                "SELECT EXISTS(SELECT count(*) AS v FROM range(3) GROUP BY CAST('bad' AS INT)) AS r",
                Ok(vec![ScalarValue::Boolean(Some(true))]),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT EXISTS(SELECT CAST('bad' AS INT) AS v FROM range(3) GROUP BY CAST('bad' AS INT) HAVING count(*)>0) AS r",
                Ok(vec![ScalarValue::Boolean(Some(true))]),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT EXISTS(SELECT CAST('bad' AS INT) AS v FROM range(3) GROUP BY CAST('bad' AS INT) HAVING CAST('bad' AS INT)>0) AS r",
                Err("Cannot cast"),
                Ok(vec![ScalarValue::Boolean(Some(false))]),
            ),
            (
                "SELECT EXISTS(SELECT sum(CAST('bad' AS INT)) AS v FROM range(3) GROUP BY id) AS r",
                Ok(vec![ScalarValue::Boolean(Some(true))]),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT EXISTS(SELECT CAST('bad' AS INT) AS v FROM range(0) GROUP BY CAST('bad' AS INT)) AS r",
                Ok(vec![ScalarValue::Boolean(Some(false))]),
                Ok(vec![ScalarValue::Boolean(Some(false))]),
            ),
            (
                "SELECT count(*) AS r FROM (SELECT CAST('bad' AS INT) AS v FROM range(0) GROUP BY CAST('bad' AS INT)) u",
                Ok(vec![ScalarValue::Int64(Some(0))]),
                Ok(vec![ScalarValue::Int64(Some(0))]),
            ),
            (
                "SELECT EXISTS(SELECT count(*) AS v FROM range(0) GROUP BY CAST('bad' AS INT)) AS r",
                Ok(vec![ScalarValue::Boolean(Some(false))]),
                Ok(vec![ScalarValue::Boolean(Some(false))]),
            ),
            (
                "SELECT EXISTS(SELECT CAST('bad' AS INT) AS v FROM range(0) GROUP BY CAST('bad' AS INT) HAVING count(*)>0) AS r",
                Ok(vec![ScalarValue::Boolean(Some(false))]),
                Ok(vec![ScalarValue::Boolean(Some(false))]),
            ),
            (
                "SELECT EXISTS(SELECT CAST('bad' AS INT) AS v FROM range(0) GROUP BY CAST('bad' AS INT) HAVING CAST('bad' AS INT)>0) AS r",
                Err("Cannot cast"),
                Ok(vec![ScalarValue::Boolean(Some(false))]),
            ),
            (
                "SELECT EXISTS(SELECT sum(CAST('bad' AS INT)) AS v FROM range(0) GROUP BY id) AS r",
                Ok(vec![ScalarValue::Boolean(Some(false))]),
                Ok(vec![ScalarValue::Boolean(Some(false))]),
            ),
            (
                "SELECT EXISTS(SELECT CAST('bad' AS INT) AS v FROM VALUES(0),(1) t(id) GROUP BY CAST('bad' AS INT)) AS r",
                Ok(vec![ScalarValue::Boolean(Some(true))]),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT count(*) AS r FROM (SELECT CAST('bad' AS INT) AS v FROM VALUES(0),(1) t(id) GROUP BY CAST('bad' AS INT)) u",
                Ok(vec![ScalarValue::Int64(Some(1))]),
                Ok(vec![ScalarValue::Int64(Some(1))]),
            ),
            (
                "SELECT EXISTS(SELECT count(*) AS v FROM VALUES(0),(1) t(id) GROUP BY CAST('bad' AS INT)) AS r",
                Ok(vec![ScalarValue::Boolean(Some(true))]),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT EXISTS(SELECT CAST('bad' AS INT) AS v FROM VALUES(0),(1) t(id) GROUP BY CAST('bad' AS INT) HAVING count(*)>0) AS r",
                Ok(vec![ScalarValue::Boolean(Some(true))]),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT EXISTS(SELECT CAST('bad' AS INT) AS v FROM VALUES(0),(1) t(id) GROUP BY CAST('bad' AS INT) HAVING CAST('bad' AS INT)>0) AS r",
                Err("Cannot cast"),
                Ok(vec![ScalarValue::Boolean(Some(false))]),
            ),
            (
                "SELECT EXISTS(SELECT sum(CAST('bad' AS INT)) AS v FROM VALUES(0),(1) t(id) GROUP BY id) AS r",
                Ok(vec![ScalarValue::Boolean(Some(true))]),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT EXISTS(SELECT 7 DIV 0 AS v FROM range(3) GROUP BY 7 DIV 0) AS r",
                Ok(vec![ScalarValue::Boolean(Some(true))]),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT count(*) AS r FROM (SELECT 7 DIV 0 AS v FROM range(3) GROUP BY 7 DIV 0) u",
                Ok(vec![ScalarValue::Int64(Some(1))]),
                Ok(vec![ScalarValue::Int64(Some(1))]),
            ),
            (
                "SELECT EXISTS(SELECT count(*) AS v FROM range(3) GROUP BY 7 DIV 0) AS r",
                Ok(vec![ScalarValue::Boolean(Some(true))]),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT EXISTS(SELECT 7 DIV 0 AS v FROM range(3) GROUP BY 7 DIV 0 HAVING count(*)>0) AS r",
                Ok(vec![ScalarValue::Boolean(Some(true))]),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT EXISTS(SELECT 7 DIV 0 AS v FROM range(3) GROUP BY 7 DIV 0 HAVING 7 DIV 0>0) AS r",
                Err("Divide by zero"),
                Ok(vec![ScalarValue::Boolean(Some(false))]),
            ),
            (
                "SELECT EXISTS(SELECT sum(7 DIV 0) AS v FROM range(3) GROUP BY id) AS r",
                Ok(vec![ScalarValue::Boolean(Some(true))]),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT EXISTS(SELECT 7 DIV 0 AS v FROM range(0) GROUP BY 7 DIV 0) AS r",
                Ok(vec![ScalarValue::Boolean(Some(false))]),
                Ok(vec![ScalarValue::Boolean(Some(false))]),
            ),
            (
                "SELECT count(*) AS r FROM (SELECT 7 DIV 0 AS v FROM range(0) GROUP BY 7 DIV 0) u",
                Ok(vec![ScalarValue::Int64(Some(0))]),
                Ok(vec![ScalarValue::Int64(Some(0))]),
            ),
            (
                "SELECT EXISTS(SELECT count(*) AS v FROM range(0) GROUP BY 7 DIV 0) AS r",
                Ok(vec![ScalarValue::Boolean(Some(false))]),
                Ok(vec![ScalarValue::Boolean(Some(false))]),
            ),
            (
                "SELECT EXISTS(SELECT 7 DIV 0 AS v FROM range(0) GROUP BY 7 DIV 0 HAVING count(*)>0) AS r",
                Ok(vec![ScalarValue::Boolean(Some(false))]),
                Ok(vec![ScalarValue::Boolean(Some(false))]),
            ),
            (
                "SELECT EXISTS(SELECT 7 DIV 0 AS v FROM range(0) GROUP BY 7 DIV 0 HAVING 7 DIV 0>0) AS r",
                Err("Divide by zero"),
                Ok(vec![ScalarValue::Boolean(Some(false))]),
            ),
            (
                "SELECT EXISTS(SELECT sum(7 DIV 0) AS v FROM range(0) GROUP BY id) AS r",
                Ok(vec![ScalarValue::Boolean(Some(false))]),
                Ok(vec![ScalarValue::Boolean(Some(false))]),
            ),
            (
                "SELECT EXISTS(SELECT 7 DIV 0 AS v FROM VALUES(0),(1) t(id) GROUP BY 7 DIV 0) AS r",
                Ok(vec![ScalarValue::Boolean(Some(true))]),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT count(*) AS r FROM (SELECT 7 DIV 0 AS v FROM VALUES(0),(1) t(id) GROUP BY 7 DIV 0) u",
                Ok(vec![ScalarValue::Int64(Some(1))]),
                Ok(vec![ScalarValue::Int64(Some(1))]),
            ),
            (
                "SELECT EXISTS(SELECT count(*) AS v FROM VALUES(0),(1) t(id) GROUP BY 7 DIV 0) AS r",
                Ok(vec![ScalarValue::Boolean(Some(true))]),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT EXISTS(SELECT 7 DIV 0 AS v FROM VALUES(0),(1) t(id) GROUP BY 7 DIV 0 HAVING count(*)>0) AS r",
                Ok(vec![ScalarValue::Boolean(Some(true))]),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT EXISTS(SELECT 7 DIV 0 AS v FROM VALUES(0),(1) t(id) GROUP BY 7 DIV 0 HAVING 7 DIV 0>0) AS r",
                Err("Divide by zero"),
                Ok(vec![ScalarValue::Boolean(Some(false))]),
            ),
            (
                "SELECT EXISTS(SELECT sum(7 DIV 0) AS v FROM VALUES(0),(1) t(id) GROUP BY id) AS r",
                Ok(vec![ScalarValue::Boolean(Some(true))]),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
        ] {
            let result = async {
                let ast = sail_sql_analyzer::parser::parse_one_statement(sql)?;
                let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
                let config = PlanConfig {
                    ansi_mode: ansi,
                    ..Default::default()
                };
                let named = PlanResolver::new(&ctx, Arc::new(config))
                    .resolve_named_plan(spec)
                    .await?;
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
                    spark_test_frame(&ctx, named.plan).collect().await?,
                )
            }
            .await;
            match if ansi { strict } else { legacy } {
                Err(fragment) => {
                    let error = result.expect_err(sql).to_string();
                    assert!(error.contains(fragment), "ANSI {ansi}: {sql}: {error}");
                }
                Ok(expected) => {
                    let mut actual = Vec::new();
                    for batch in result? {
                        for row in 0..batch.num_rows() {
                            actual.push(ScalarValue::try_from_array(batch.column(0), row)?);
                        }
                    }
                    assert_eq!(actual, expected, "ANSI {ansi}: {sql}");
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn group_only_limit_preserves_live_and_local_errors()
-> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use crate::{config::PlanConfig, physical_plan::SparkQueryPlanner, resolver::PlanResolver};
    use datafusion::arrow::{
        datatypes::{DataType, Schema},
        record_batch::RecordBatch,
    };
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::SessionContext;
    let ctx = SessionContext::new_with_state(
        SessionStateBuilder::new()
            .with_default_features()
            .with_analyzer_rule(Arc::new(PrepareSparkExpressions))
            .with_query_planner(Arc::new(SparkQueryPlanner))
            .build(),
    );
    ctx.register_batch(
        "group_input",
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![datafusion::arrow::datatypes::Field::new(
                "a",
                DataType::Float64,
                false,
            )])),
            vec![Arc::new(datafusion::arrow::array::Float64Array::from(
                vec![1.0, 2.0, 3.0],
            ))],
        )?,
    )?;
    for ansi in [true, false, true] {
        for (sql, strict, legacy) in [
            (
                "SELECT EXISTS(SELECT id DIV 0 AS v FROM range(3) GROUP BY id DIV 0) AS r",
                Ok(vec![ScalarValue::Boolean(Some(true))]),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT count(*) AS r FROM (SELECT id DIV 0 AS v FROM range(3) GROUP BY id DIV 0 LIMIT 1) t",
                Ok(vec![ScalarValue::Int64(Some(1))]),
                Ok(vec![ScalarValue::Int64(Some(1))]),
            ),
            (
                "SELECT EXISTS(SELECT id DIV 0+1 AS v FROM range(3) GROUP BY id DIV 0) AS r",
                Ok(vec![ScalarValue::Boolean(Some(true))]),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT EXISTS(SELECT count(*) AS v FROM range(3) GROUP BY id DIV 0) AS r",
                Ok(vec![ScalarValue::Boolean(Some(true))]),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT EXISTS(SELECT sum(7 DIV 0) AS v FROM range(3) GROUP BY id DIV 0) AS r",
                Ok(vec![ScalarValue::Boolean(Some(true))]),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT count(*) AS r FROM (SELECT id DIV 0 AS v FROM range(3) GROUP BY id DIV 0 HAVING count(*)>0 LIMIT 1) t",
                Err("Divide by zero"),
                Ok(vec![ScalarValue::Int64(Some(1))]),
            ),
            (
                "SELECT EXISTS(SELECT id DIV 0 AS v FROM range(0) GROUP BY id DIV 0) AS r",
                Ok(vec![ScalarValue::Boolean(Some(false))]),
                Ok(vec![ScalarValue::Boolean(Some(false))]),
            ),
            (
                "SELECT count(*) AS r FROM (SELECT id DIV 0 AS v FROM range(0) GROUP BY id DIV 0 LIMIT 1) t",
                Ok(vec![ScalarValue::Int64(Some(0))]),
                Ok(vec![ScalarValue::Int64(Some(0))]),
            ),
            (
                "SELECT EXISTS(SELECT id DIV 0+1 AS v FROM range(0) GROUP BY id DIV 0) AS r",
                Ok(vec![ScalarValue::Boolean(Some(false))]),
                Ok(vec![ScalarValue::Boolean(Some(false))]),
            ),
            (
                "SELECT EXISTS(SELECT count(*) AS v FROM range(0) GROUP BY id DIV 0) AS r",
                Ok(vec![ScalarValue::Boolean(Some(false))]),
                Ok(vec![ScalarValue::Boolean(Some(false))]),
            ),
            (
                "SELECT EXISTS(SELECT sum(7 DIV 0) AS v FROM range(0) GROUP BY id DIV 0) AS r",
                Ok(vec![ScalarValue::Boolean(Some(false))]),
                Ok(vec![ScalarValue::Boolean(Some(false))]),
            ),
            (
                "SELECT count(*) AS r FROM (SELECT id DIV 0 AS v FROM range(0) GROUP BY id DIV 0 HAVING count(*)>0 LIMIT 1) t",
                Ok(vec![ScalarValue::Int64(Some(0))]),
                Ok(vec![ScalarValue::Int64(Some(0))]),
            ),
            (
                "SELECT count(*) AS r FROM (SELECT id DIV 0 AS v FROM range(0) GROUP BY id DIV 0 HAVING id DIV 0 IS NULL LIMIT 1) t",
                Ok(vec![ScalarValue::Int64(Some(0))]),
                Ok(vec![ScalarValue::Int64(Some(0))]),
            ),
            (
                "SELECT EXISTS(SELECT id DIV 0 AS v FROM VALUES(0),(1) t(id) GROUP BY id DIV 0) AS r",
                Err("Divide by zero"),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT count(*) AS r FROM (SELECT id DIV 0 AS v FROM VALUES(0),(1) t(id) GROUP BY id DIV 0 LIMIT 1) t",
                Err("Divide by zero"),
                Ok(vec![ScalarValue::Int64(Some(1))]),
            ),
            (
                "SELECT EXISTS(SELECT id DIV 0+1 AS v FROM VALUES(0),(1) t(id) GROUP BY id DIV 0) AS r",
                Err("Divide by zero"),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT EXISTS(SELECT count(*) AS v FROM VALUES(0),(1) t(id) GROUP BY id DIV 0) AS r",
                Err("Divide by zero"),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT EXISTS(SELECT sum(7 DIV 0) AS v FROM VALUES(0),(1) t(id) GROUP BY id DIV 0) AS r",
                Err("Divide by zero"),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT count(*) AS r FROM (SELECT id DIV 0 AS v FROM VALUES(0),(1) t(id) GROUP BY id DIV 0 HAVING count(*)>0 LIMIT 1) t",
                Err("Divide by zero"),
                Ok(vec![ScalarValue::Int64(Some(1))]),
            ),
            (
                "SELECT count(*) AS r FROM (SELECT id DIV 0 AS v FROM VALUES(0),(1) t(id) GROUP BY id DIV 0 HAVING id DIV 0 IS NULL LIMIT 1) t",
                Err("Divide by zero"),
                Ok(vec![ScalarValue::Int64(Some(1))]),
            ),
            (
                "SELECT EXISTS(SELECT CAST(CASE WHEN id=0 THEN 'bad' ELSE '1' END AS INT) AS v FROM range(3) GROUP BY CAST(CASE WHEN id=0 THEN 'bad' ELSE '1' END AS INT)) AS r",
                Ok(vec![ScalarValue::Boolean(Some(true))]),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT count(*) AS r FROM (SELECT CAST(CASE WHEN id=0 THEN 'bad' ELSE '1' END AS INT) AS v FROM range(3) GROUP BY CAST(CASE WHEN id=0 THEN 'bad' ELSE '1' END AS INT) LIMIT 1) t",
                Ok(vec![ScalarValue::Int64(Some(1))]),
                Ok(vec![ScalarValue::Int64(Some(1))]),
            ),
            (
                "SELECT EXISTS(SELECT CAST(CASE WHEN id=0 THEN 'bad' ELSE '1' END AS INT)+1 AS v FROM range(3) GROUP BY CAST(CASE WHEN id=0 THEN 'bad' ELSE '1' END AS INT)) AS r",
                Ok(vec![ScalarValue::Boolean(Some(true))]),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT EXISTS(SELECT count(*) AS v FROM range(3) GROUP BY CAST(CASE WHEN id=0 THEN 'bad' ELSE '1' END AS INT)) AS r",
                Ok(vec![ScalarValue::Boolean(Some(true))]),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT EXISTS(SELECT sum(7 DIV 0) AS v FROM range(3) GROUP BY CAST(CASE WHEN id=0 THEN 'bad' ELSE '1' END AS INT)) AS r",
                Ok(vec![ScalarValue::Boolean(Some(true))]),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT count(*) AS r FROM (SELECT CAST(CASE WHEN id=0 THEN 'bad' ELSE '1' END AS INT) AS v FROM range(3) GROUP BY CAST(CASE WHEN id=0 THEN 'bad' ELSE '1' END AS INT) HAVING count(*)>0 LIMIT 1) t",
                Err("Cannot cast"),
                Ok(vec![ScalarValue::Int64(Some(1))]),
            ),
            (
                "SELECT count(*) AS r FROM (SELECT CAST(CASE WHEN id=0 THEN 'bad' ELSE '1' END AS INT) AS v FROM range(3) GROUP BY CAST(CASE WHEN id=0 THEN 'bad' ELSE '1' END AS INT) HAVING CAST(CASE WHEN id=0 THEN 'bad' ELSE '1' END AS INT) IS NULL LIMIT 1) t",
                Err("Cannot cast"),
                Ok(vec![ScalarValue::Int64(Some(1))]),
            ),
            (
                "SELECT EXISTS(SELECT CAST(CASE WHEN id=0 THEN 'bad' ELSE '1' END AS INT) AS v FROM range(0) GROUP BY CAST(CASE WHEN id=0 THEN 'bad' ELSE '1' END AS INT)) AS r",
                Ok(vec![ScalarValue::Boolean(Some(false))]),
                Ok(vec![ScalarValue::Boolean(Some(false))]),
            ),
            (
                "SELECT count(*) AS r FROM (SELECT CAST(CASE WHEN id=0 THEN 'bad' ELSE '1' END AS INT) AS v FROM range(0) GROUP BY CAST(CASE WHEN id=0 THEN 'bad' ELSE '1' END AS INT) LIMIT 1) t",
                Ok(vec![ScalarValue::Int64(Some(0))]),
                Ok(vec![ScalarValue::Int64(Some(0))]),
            ),
            (
                "SELECT EXISTS(SELECT CAST(CASE WHEN id=0 THEN 'bad' ELSE '1' END AS INT)+1 AS v FROM range(0) GROUP BY CAST(CASE WHEN id=0 THEN 'bad' ELSE '1' END AS INT)) AS r",
                Ok(vec![ScalarValue::Boolean(Some(false))]),
                Ok(vec![ScalarValue::Boolean(Some(false))]),
            ),
            (
                "SELECT EXISTS(SELECT count(*) AS v FROM range(0) GROUP BY CAST(CASE WHEN id=0 THEN 'bad' ELSE '1' END AS INT)) AS r",
                Ok(vec![ScalarValue::Boolean(Some(false))]),
                Ok(vec![ScalarValue::Boolean(Some(false))]),
            ),
            (
                "SELECT EXISTS(SELECT sum(7 DIV 0) AS v FROM range(0) GROUP BY CAST(CASE WHEN id=0 THEN 'bad' ELSE '1' END AS INT)) AS r",
                Ok(vec![ScalarValue::Boolean(Some(false))]),
                Ok(vec![ScalarValue::Boolean(Some(false))]),
            ),
            (
                "SELECT count(*) AS r FROM (SELECT CAST(CASE WHEN id=0 THEN 'bad' ELSE '1' END AS INT) AS v FROM range(0) GROUP BY CAST(CASE WHEN id=0 THEN 'bad' ELSE '1' END AS INT) HAVING count(*)>0 LIMIT 1) t",
                Ok(vec![ScalarValue::Int64(Some(0))]),
                Ok(vec![ScalarValue::Int64(Some(0))]),
            ),
            (
                "SELECT count(*) AS r FROM (SELECT CAST(CASE WHEN id=0 THEN 'bad' ELSE '1' END AS INT) AS v FROM range(0) GROUP BY CAST(CASE WHEN id=0 THEN 'bad' ELSE '1' END AS INT) HAVING CAST(CASE WHEN id=0 THEN 'bad' ELSE '1' END AS INT) IS NULL LIMIT 1) t",
                Ok(vec![ScalarValue::Int64(Some(0))]),
                Ok(vec![ScalarValue::Int64(Some(0))]),
            ),
            (
                "SELECT EXISTS(SELECT CAST(CASE WHEN id=0 THEN 'bad' ELSE '1' END AS INT) AS v FROM VALUES(0),(1) t(id) GROUP BY CAST(CASE WHEN id=0 THEN 'bad' ELSE '1' END AS INT)) AS r",
                Err("Cannot cast"),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT count(*) AS r FROM (SELECT CAST(CASE WHEN id=0 THEN 'bad' ELSE '1' END AS INT) AS v FROM VALUES(0),(1) t(id) GROUP BY CAST(CASE WHEN id=0 THEN 'bad' ELSE '1' END AS INT) LIMIT 1) t",
                Err("Cannot cast"),
                Ok(vec![ScalarValue::Int64(Some(1))]),
            ),
            (
                "SELECT EXISTS(SELECT CAST(CASE WHEN id=0 THEN 'bad' ELSE '1' END AS INT)+1 AS v FROM VALUES(0),(1) t(id) GROUP BY CAST(CASE WHEN id=0 THEN 'bad' ELSE '1' END AS INT)) AS r",
                Err("Cannot cast"),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT EXISTS(SELECT count(*) AS v FROM VALUES(0),(1) t(id) GROUP BY CAST(CASE WHEN id=0 THEN 'bad' ELSE '1' END AS INT)) AS r",
                Err("Cannot cast"),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT EXISTS(SELECT sum(7 DIV 0) AS v FROM VALUES(0),(1) t(id) GROUP BY CAST(CASE WHEN id=0 THEN 'bad' ELSE '1' END AS INT)) AS r",
                Err("Cannot cast"),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT count(*) AS r FROM (SELECT CAST(CASE WHEN id=0 THEN 'bad' ELSE '1' END AS INT) AS v FROM VALUES(0),(1) t(id) GROUP BY CAST(CASE WHEN id=0 THEN 'bad' ELSE '1' END AS INT) HAVING count(*)>0 LIMIT 1) t",
                Err("Cannot cast"),
                Ok(vec![ScalarValue::Int64(Some(1))]),
            ),
            (
                "SELECT count(*) AS r FROM (SELECT CAST(CASE WHEN id=0 THEN 'bad' ELSE '1' END AS INT) AS v FROM VALUES(0),(1) t(id) GROUP BY CAST(CASE WHEN id=0 THEN 'bad' ELSE '1' END AS INT) HAVING CAST(CASE WHEN id=0 THEN 'bad' ELSE '1' END AS INT) IS NULL LIMIT 1) t",
                Err("Cannot cast"),
                Ok(vec![ScalarValue::Int64(Some(1))]),
            ),
            (
                "SELECT EXISTS(SELECT a / 0D FROM group_input GROUP BY a / 0D) AS r",
                Ok(vec![ScalarValue::Boolean(Some(true))]),
                Ok(vec![ScalarValue::Boolean(Some(true))]),
            ),
            (
                "SELECT count(*) AS r FROM (SELECT a / 0D FROM group_input GROUP BY a / 0D LIMIT 1) t",
                Ok(vec![ScalarValue::Int64(Some(1))]),
                Ok(vec![ScalarValue::Int64(Some(1))]),
            ),
        ] {
            let result = async {
                let ast = sail_sql_analyzer::parser::parse_one_statement(sql)?;
                let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
                let config = PlanConfig {
                    ansi_mode: ansi,
                    ..Default::default()
                };
                let named = PlanResolver::new(&ctx, Arc::new(config))
                    .resolve_named_plan(spec)
                    .await?;
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
                    spark_test_frame(&ctx, named.plan).collect().await?,
                )
            }
            .await;
            match if ansi { strict } else { legacy } {
                Err(fragment) => {
                    let error = result.expect_err(sql).to_string();
                    assert!(error.contains(fragment), "ANSI {ansi}: {sql}: {error}");
                }
                Ok(expected) => {
                    let mut actual = Vec::new();
                    for batch in result? {
                        for row in 0..batch.num_rows() {
                            actual.push(ScalarValue::try_from_array(batch.column(0), row)?);
                        }
                    }
                    assert_eq!(actual, expected, "ANSI {ansi}: {sql}");
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn correlated_exists_prunes_unused_cast()
-> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use crate::{config::PlanConfig, physical_plan::SparkQueryPlanner, resolver::PlanResolver};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::SessionContext;

    let ctx = SessionContext::new_with_state(
        SessionStateBuilder::new()
            .with_default_features()
            .with_analyzer_rule(Arc::new(PrepareSparkExpressions))
            .with_query_planner(Arc::new(SparkQueryPlanner))
            .build(),
    );
    for ansi in [true, false] {
        for data_type in ["INT", "DOUBLE", "DECIMAL(10,2)"] {
            for negated in [false, true] {
                let prefix = if negated { "NOT " } else { "" };
                for (distinct, predicate, fails, present) in [
                    ("", "id=k", false, [false, true, false]),
                    ("DISTINCT ", "id=k", true, [false, true, false]),
                    ("", "id=k AND false", true, [false; 3]),
                    ("", "id=k AND (1=2)", false, [false; 3]),
                    ("", "id=k AND CAST(NULL AS BOOLEAN)", false, [false; 3]),
                ] {
                    let sql = format!(
                        "SELECT k, {prefix}EXISTS(SELECT {distinct}CAST('bad' AS {data_type})                          FROM range(3) WHERE {predicate}) AS r                          FROM VALUES(0),(4),(CAST(NULL AS BIGINT)) t(k) ORDER BY k NULLS FIRST"
                    );
                    let ast = sail_sql_analyzer::parser::parse_one_statement(&sql)?;
                    let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
                    let config = PlanConfig {
                        ansi_mode: ansi,
                        ..Default::default()
                    };
                    let named = PlanResolver::new(&ctx, Arc::new(config))
                        .resolve_named_plan(spec)
                        .await?;
                    let result = spark_test_frame(&ctx, named.plan).collect().await;
                    if ansi && fails {
                        assert!(
                            result
                                .unwrap_err()
                                .to_string()
                                .to_lowercase()
                                .contains("cast"),
                            "{sql}"
                        );
                    } else {
                        let batches = result?;
                        let actual: Vec<_> = batches
                            .iter()
                            .flat_map(|batch| batch.column(1).as_boolean().iter())
                            .collect();
                        let expected: Vec<_> =
                            present.into_iter().map(|v| Some(v != negated)).collect();
                        assert_eq!(actual, expected, "{sql}, ANSI={ansi}");
                    }
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn correlation_free_exists_prepares_output()
-> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use crate::{config::PlanConfig, physical_plan::SparkQueryPlanner, resolver::PlanResolver};
    use datafusion::arrow::array::AsArray;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::SessionContext;

    let ctx = SessionContext::new_with_state(
        SessionStateBuilder::new()
            .with_default_features()
            .with_analyzer_rule(Arc::new(PrepareSparkExpressions))
            .with_query_planner(Arc::new(SparkQueryPlanner))
            .build(),
    );
    for ansi in [true, false] {
        for expression in [
            "CAST('bad' AS INT)",
            "CAST('bad' AS DOUBLE)",
            "CAST('bad' AS DECIMAL(10,2))",
            "7 DIV 0",
            "7.0 / 0",
            "7 % 0",
        ] {
            for negated in [false, true] {
                let prefix = if negated { "NOT " } else { "" };
                for (input, predicate, fails, present) in [
                    (
                        "VALUES(0),(1),(2) t(id)",
                        "id=k AND false",
                        false,
                        [false; 3],
                    ),
                    ("range(3)", "id=k AND false", true, [false; 3]),
                    ("range(3)", "id=k", false, [false, true, false]),
                    ("range(3)", "id=k AND (1=2)", false, [false; 3]),
                    (
                        "range(3)",
                        "id=k AND CAST(NULL AS BOOLEAN)",
                        false,
                        [false; 3],
                    ),
                    (
                        "VALUES(0),(1),(2) t(id)",
                        "id=k",
                        true,
                        [false, true, false],
                    ),
                ] {
                    let sql = format!(
                        "SELECT k, {prefix}EXISTS(SELECT {expression} FROM {input} WHERE {predicate}) AS r \
                         FROM VALUES(0),(4),(CAST(NULL AS BIGINT)) o(k) ORDER BY k NULLS FIRST"
                    );
                    let ast = sail_sql_analyzer::parser::parse_one_statement(&sql)?;
                    let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
                    let config = PlanConfig {
                        ansi_mode: ansi,
                        ..Default::default()
                    };
                    let named = PlanResolver::new(&ctx, Arc::new(config))
                        .resolve_named_plan(spec)
                        .await?;
                    let result = spark_test_frame(&ctx, named.plan).collect().await;
                    if ansi && fails {
                        let error = result.unwrap_err().to_string().to_lowercase();
                        let cause = if expression.starts_with("CAST") {
                            "cast"
                        } else if expression == "7 % 0" {
                            "[remainder_by_zero]"
                        } else {
                            "divide by zero"
                        };
                        assert!(error.contains(cause), "{sql}: {error}");
                    } else {
                        let batches = result?;
                        let actual: Vec<_> = batches
                            .iter()
                            .flat_map(|batch| batch.column(1).as_boolean().iter())
                            .collect();
                        let expected: Vec<_> =
                            present.into_iter().map(|v| Some(v != negated)).collect();
                        assert_eq!(actual, expected, "{sql}, ANSI={ansi}");
                    }
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn empty_local_projection_precedes_scalar_preparation()
-> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use crate::{config::PlanConfig, physical_plan::SparkQueryPlanner, resolver::PlanResolver};
    use datafusion::arrow::array::AsArray;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::SessionContext;

    let ctx = SessionContext::new_with_state(
        SessionStateBuilder::new()
            .with_default_features()
            .with_analyzer_rule(Arc::new(PrepareSparkExpressions))
            .with_query_planner(Arc::new(SparkQueryPlanner))
            .build(),
    );
    for ansi in [true, false] {
        for expression in [
            "CAST('bad' AS INT)",
            "CAST('bad' AS DOUBLE)",
            "CAST('bad' AS DECIMAL(10,2))",
            "7 DIV 0",
            "7.0 / 0",
            "7 % 0",
        ] {
            for context in ["exists", "not_exists", "in", "not_in", "scalar_null"] {
                for (input, predicate, fails, present) in [
                    (
                        "VALUES(0),(1),(2) t(id)",
                        "id=k AND false",
                        false,
                        [false; 3],
                    ),
                    (
                        "VALUES(0),(1),(2) t(id)",
                        "id=k",
                        true,
                        [false, true, false],
                    ),
                    ("range(3)", "id=k AND false", true, [false; 3]),
                ] {
                    if fails && !matches!(context, "exists" | "not_exists") {
                        continue;
                    }
                    let subquery = format!(
                        "SELECT (SELECT {expression} FROM range(1)) FROM {input} WHERE {predicate}"
                    );
                    let predicate = match context {
                        "exists" => format!("EXISTS({subquery})"),
                        "not_exists" => format!("NOT EXISTS({subquery})"),
                        "in" => format!("k IN ({subquery})"),
                        "not_in" => format!("k NOT IN ({subquery})"),
                        _ => format!("({subquery}) IS NULL"),
                    };
                    let sql = format!(
                        "SELECT k, {predicate} AS r FROM VALUES(0),(4),(CAST(NULL AS BIGINT)) o(k) \
                         ORDER BY k NULLS FIRST"
                    );
                    let ast = sail_sql_analyzer::parser::parse_one_statement(&sql)?;
                    let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
                    let config = PlanConfig {
                        ansi_mode: ansi,
                        ..Default::default()
                    };
                    let named = PlanResolver::new(&ctx, Arc::new(config))
                        .resolve_named_plan(spec)
                        .await?;
                    let result = spark_test_frame(&ctx, named.plan).collect().await;
                    if ansi && fails {
                        let error = result.unwrap_err().to_string().to_lowercase();
                        let cause = if expression.starts_with("CAST") {
                            "cast"
                        } else if expression == "7 % 0" {
                            "[remainder_by_zero]"
                        } else {
                            "divide by zero"
                        };
                        assert!(error.contains(cause), "{sql}: {error}");
                    } else {
                        let batches = result?;
                        let actual: Vec<_> = batches
                            .iter()
                            .flat_map(|batch| batch.column(1).as_boolean().iter())
                            .collect();
                        let expected: Vec<_> = present
                            .into_iter()
                            .map(|v| {
                                Some(
                                    v != matches!(context, "not_exists" | "not_in" | "scalar_null"),
                                )
                            })
                            .collect();
                        assert_eq!(actual, expected, "{sql}, ANSI={ansi}");
                    }
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn empty_group_preparation_preserves_grand_totals()
-> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use crate::{config::PlanConfig, physical_plan::SparkQueryPlanner, resolver::PlanResolver};
    use datafusion::arrow::array::AsArray;
    use datafusion::common::ScalarValue;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::SessionContext;

    let ctx = SessionContext::new_with_state(
        SessionStateBuilder::new()
            .with_default_features()
            .with_analyzer_rule(Arc::new(PrepareSparkExpressions))
            .with_query_planner(Arc::new(SparkQueryPlanner))
            .build(),
    );
    for ansi in [true, false] {
        for expression in [
            "CAST('bad' AS INT)",
            "CAST('bad' AS DOUBLE)",
            "CAST('bad' AS DECIMAL(10,2))",
            "7 DIV 0",
            "7.0 / 0",
            "7 % 0",
        ] {
            for negated in [false, true] {
                let prefix = if negated { "NOT " } else { "" };
                for (input, predicate, grouping, extra, fails, present) in [
                    (
                        "VALUES(0),(1),(2) t(id)",
                        "id=k AND false",
                        " GROUP BY id",
                        "",
                        false,
                        [false; 3],
                    ),
                    (
                        "VALUES(0),(1),(2) t(id)",
                        "id=k",
                        " GROUP BY id",
                        "",
                        true,
                        [false, true, false],
                    ),
                    (
                        "range(3)",
                        "id=k AND false",
                        " GROUP BY id",
                        "",
                        true,
                        [false; 3],
                    ),
                    (
                        "VALUES(0),(1),(2) t(id)",
                        "false",
                        "",
                        ", count(*)",
                        true,
                        [true; 3],
                    ),
                ] {
                    let sql = format!(
                        "SELECT k, {prefix}EXISTS(SELECT (SELECT {expression} FROM range(1)){extra} \
                         FROM {input} WHERE {predicate}{grouping}) AS r \
                         FROM VALUES(0),(4),(CAST(NULL AS BIGINT)) o(k) ORDER BY k NULLS FIRST"
                    );
                    let ast = sail_sql_analyzer::parser::parse_one_statement(&sql)?;
                    let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
                    let config = PlanConfig {
                        ansi_mode: ansi,
                        ..Default::default()
                    };
                    let named = PlanResolver::new(&ctx, Arc::new(config))
                        .resolve_named_plan(spec)
                        .await?;
                    let result = spark_test_frame(&ctx, named.plan).collect().await;
                    if ansi && fails {
                        let error = result.unwrap_err().to_string().to_lowercase();
                        let cause = if expression.starts_with("CAST") {
                            "cast"
                        } else if expression == "7 % 0" {
                            "[remainder_by_zero]"
                        } else {
                            "divide by zero"
                        };
                        assert!(error.contains(cause), "{sql}: {error}");
                    } else {
                        let batches = result?;
                        let actual: Vec<_> = batches
                            .iter()
                            .flat_map(|batch| batch.column(1).as_boolean().iter())
                            .collect();
                        let expected: Vec<_> =
                            present.into_iter().map(|v| Some(v != negated)).collect();
                        assert_eq!(actual, expected, "{sql}, ANSI={ansi}");
                    }
                }
            }
        }
        for (grouping, total) in [
            ("", true),
            (" GROUP BY id", false),
            (" GROUP BY GROUPING SETS(())", true),
            (" GROUP BY GROUPING SETS((id),())", true),
            (" GROUP BY ROLLUP(id)", true),
            (" GROUP BY CUBE(id)", true),
        ] {
            let sql = format!(
                "SELECT count(CAST('7' AS INT)) AS n FROM VALUES(0),(1) t(id) WHERE false{grouping}"
            );
            let ast = sail_sql_analyzer::parser::parse_one_statement(&sql)?;
            let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
            let config = PlanConfig {
                ansi_mode: ansi,
                ..Default::default()
            };
            let named = PlanResolver::new(&ctx, Arc::new(config))
                .resolve_named_plan(spec)
                .await?;
            let batches = spark_test_frame(&ctx, named.plan).collect().await?;
            let mut actual = Vec::new();
            for batch in batches {
                for row in 0..batch.num_rows() {
                    actual.push(ScalarValue::try_from_array(batch.column(0), row)?);
                }
            }
            let expected = if total {
                vec![ScalarValue::Int64(Some(0))]
            } else {
                vec![]
            };
            assert_eq!(actual, expected, "{sql}, ANSI={ansi}");
        }
    }
    Ok(())
}

#[tokio::test]
async fn empty_operators_precede_scalar_children()
-> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use crate::{config::PlanConfig, physical_plan::SparkQueryPlanner, resolver::PlanResolver};
    use datafusion::arrow::array::AsArray;
    use datafusion::common::ScalarValue;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::SessionContext;

    let ctx = SessionContext::new_with_state(
        SessionStateBuilder::new()
            .with_default_features()
            .with_analyzer_rule(Arc::new(PrepareSparkExpressions))
            .with_query_planner(Arc::new(SparkQueryPlanner))
            .build(),
    );
    for ansi in [true, false] {
        for expression in [
            "CAST('bad' AS INT)",
            "CAST('bad' AS DOUBLE)",
            "CAST('bad' AS DECIMAL(10,2))",
            "7 DIV 0",
            "7.0 / 0",
            "7 % 0",
        ] {
            for (input, predicate, fails, live) in [
                ("VALUES(0),(1),(2) t(id)", "id=k AND false", false, false),
                ("VALUES(0),(1),(2) t(id)", "id=k", true, true),
                ("range(3)", "id=k AND false", true, false),
            ] {
                let scalar = format!("(SELECT {expression} FROM range(1))");
                for (subquery, has_groups) in [
                    (
                        format!(
                            "SELECT id FROM {input} WHERE {predicate} GROUP BY id HAVING {scalar}>0"
                        ),
                        false,
                    ),
                    (
                        format!(
                            "SELECT count({scalar}) FROM {input} WHERE {predicate} GROUP BY id"
                        ),
                        live,
                    ),
                    (
                        format!(
                            "SELECT id FROM (SELECT id FROM {input} WHERE {predicate}) t WHERE {scalar}>0"
                        ),
                        false,
                    ),
                ] {
                    for negated in [false, true] {
                        let prefix = if negated { "NOT " } else { "" };
                        let sql = format!(
                            "SELECT k, {prefix}EXISTS({subquery}) AS r \
                             FROM VALUES(0),(4),(CAST(NULL AS BIGINT)) o(k) ORDER BY k NULLS FIRST"
                        );
                        let ast = sail_sql_analyzer::parser::parse_one_statement(&sql)?;
                        let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
                        let config = PlanConfig {
                            ansi_mode: ansi,
                            ..Default::default()
                        };
                        let named = PlanResolver::new(&ctx, Arc::new(config))
                            .resolve_named_plan(spec)
                            .await?;
                        let result = spark_test_frame(&ctx, named.plan).collect().await;
                        if ansi && fails {
                            let error = result.unwrap_err().to_string().to_lowercase();
                            let cause = if expression.starts_with("CAST") {
                                "cast"
                            } else if expression == "7 % 0" {
                                "[remainder_by_zero]"
                            } else {
                                "divide by zero"
                            };
                            assert!(error.contains(cause), "{sql}: {error}");
                        } else {
                            let batches = result?;
                            let actual: Vec<_> = batches
                                .iter()
                                .flat_map(|batch| batch.column(1).as_boolean().iter())
                                .collect();
                            let expected: Vec<_> = [false, has_groups, false]
                                .into_iter()
                                .map(|v| Some(v != negated))
                                .collect();
                            assert_eq!(actual, expected, "{sql}, ANSI={ansi}");
                        }
                    }
                }
            }
        }
        for (grouping, total) in [
            ("", true),
            (" GROUP BY id", false),
            (" GROUP BY GROUPING SETS(())", true),
            (" GROUP BY GROUPING SETS((id),())", true),
            (" GROUP BY ROLLUP(id)", true),
            (" GROUP BY CUBE(id)", true),
        ] {
            let sql = format!(
                "SELECT count((SELECT CAST('7' AS INT) FROM range(1))) AS n \
                 FROM VALUES(0),(1) t(id) WHERE false{grouping}"
            );
            let ast = sail_sql_analyzer::parser::parse_one_statement(&sql)?;
            let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
            let config = PlanConfig {
                ansi_mode: ansi,
                ..Default::default()
            };
            let named = PlanResolver::new(&ctx, Arc::new(config))
                .resolve_named_plan(spec)
                .await?;
            let physical = spark_test_frame(&ctx, named.plan)
                .create_physical_plan()
                .await?;
            assert!(
                !physical.schema().field(0).is_nullable(),
                "{sql}: COUNT must stay nonnullable"
            );
            let batches = datafusion::physical_plan::collect(physical, ctx.task_ctx()).await?;
            let mut actual = Vec::new();
            for batch in batches {
                for row in 0..batch.num_rows() {
                    actual.push(ScalarValue::try_from_array(batch.column(0), row)?);
                }
            }
            let expected = if total {
                vec![ScalarValue::Int64(Some(0))]
            } else {
                vec![]
            };
            assert_eq!(actual, expected, "{sql}, ANSI={ansi}");
        }
    }
    Ok(())
}

#[tokio::test]
async fn local_decimal_cast_preserves_source_error_order()
-> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use crate::{config::PlanConfig, physical_plan::SparkQueryPlanner, resolver::PlanResolver};
    use datafusion::arrow::array::AsArray;
    use datafusion::arrow::datatypes::{DataType, Decimal128Type};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::{SessionConfig, SessionContext};

    for batch_size in [1, 2, 4] {
        let ctx = SessionContext::new_with_state(
            SessionStateBuilder::new()
                .with_config(
                    SessionConfig::new()
                        .with_batch_size(batch_size)
                        .with_target_partitions(2),
                )
                .with_default_features()
                .with_analyzer_rule(Arc::new(PrepareSparkExpressions))
                .with_query_planner(Arc::new(SparkQueryPlanner))
                .build(),
        );
        for reverse in [false, true] {
            let positive = "(1,CAST('1' AS DECIMAL(38,0)))";
            let negative = "(2,CAST('-1' AS DECIMAL(38,0)))";
            let middle = if reverse {
                format!("{negative},{positive}")
            } else {
                format!("{positive},{negative}")
            };
            for ansi in [true, false] {
                for cast in ["CAST", "TRY_CAST"] {
                    let sql = format!(
                        "SELECT {cast}(v AS DECIMAL(38,38)) AS r FROM VALUES \
                         (0,CAST('0' AS DECIMAL(38,0))),{middle},\
                         (3,CAST(NULL AS DECIMAL(38,0))) t(id,v) ORDER BY id"
                    );
                    let ast = sail_sql_analyzer::parser::parse_one_statement(&sql)?;
                    let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
                    let config = PlanConfig {
                        ansi_mode: ansi,
                        ..Default::default()
                    };
                    let named = PlanResolver::new(&ctx, Arc::new(config))
                        .resolve_named_plan(spec)
                        .await?;
                    let physical = async {
                        ctx.execute_logical_plan(named.plan)
                            .await?
                            .create_physical_plan()
                            .await
                    }
                    .await;
                    if ansi && cast == "CAST" {
                        let error = physical
                            .expect_err("local CAST must fail before physical execution")
                            .to_string();
                        assert!(
                            error.starts_with("preserve_spark_expression_errors\ncaused by\n"),
                            "{error}"
                        );
                        let value = if reverse {
                            "-1.00000000000000000000000000000000000000"
                        } else {
                            "1.00000000000000000000000000000000000000"
                        };
                        assert!(
                            error.contains(&format!(": {value} is too ")),
                            "{sql}: {error}"
                        );
                    } else {
                        let physical = physical?;
                        assert_eq!(
                            physical.schema().field(0).data_type(),
                            &DataType::Decimal128(38, 38)
                        );
                        let batches =
                            datafusion::physical_plan::collect(physical, ctx.task_ctx()).await?;
                        let mut actual = Vec::new();
                        for batch in batches {
                            actual.extend(batch.column(0).as_primitive::<Decimal128Type>().iter());
                        }
                        assert_eq!(
                            actual,
                            vec![Some(0), None, None, None],
                            "{sql}, ANSI={ansi}"
                        );
                    }
                }
            }
        }
    }
    Ok(())
}
