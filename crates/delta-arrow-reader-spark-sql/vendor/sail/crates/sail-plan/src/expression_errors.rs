// Local Spark planning adapter. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md.
use std::sync::Arc;

use datafusion::arrow::array::AsArray;
use datafusion::arrow::compute::filter_record_batch;
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::functions::expr_fn;
use datafusion::logical_expr::execution_props::ExecutionProps;
use datafusion::optimizer::optimize_projections::OptimizeProjections;
use datafusion::optimizer::simplify_expressions::{ExprSimplifier, prune_null};
use datafusion::optimizer::{AnalyzerRule, OptimizerContext, OptimizerRule};
use datafusion::physical_expr::create_physical_expr;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{DFSchema, DFSchemaRef, Result, ScalarValue};
use datafusion_expr::utils::{conjunction, split_conjunction_owned};
use datafusion_expr::{ColumnarValue, Expr, ExprSchemable, LogicalPlan, Operator, lit};

use crate::function::scalar::conditional::SparkNullIf;
use crate::function::scalar::math::{
    FusedDecimalDivide, SparkCheckedDivide, SparkCheckedModulo, SparkIntegerDivide,
    SparkNullIfZero, guard_cast_arithmetic, integer_divide_args,
};
use datafusion::functions::math::round::RoundFunc;
use sail_function::scalar::array::array_contains::ArrayContains;
use sail_function::scalar::math::spark_checked_arithmetic::SparkCheckedArithmetic;
use sail_function::scalar::math::{
    spark_bround::SparkBRound, spark_decimal_round::SparkDecimalRound,
    spark_float_round::SparkFloatRound, spark_integer_round::SparkIntegerRound,
};
use sail_function::scalar::spark_cast_string_to_decimal::SparkCastStringToDecimal;

/// Preserve early expression errors before NULL and empty-result rewrites.
#[derive(Debug)]
pub struct PreserveSparkExpressionErrors;

fn null_literal(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(value, _) => value.is_null(),
        Expr::Cast(cast) => null_literal(&cast.expr),
        Expr::TryCast(cast) => null_literal(&cast.expr),
        Expr::Alias(alias) => null_literal(&alias.expr),
        Expr::ScalarFunction(f)
            if (f.func.inner().is::<SparkNullIfZero>()
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

fn projected_null(expr: &Expr, input: &LogicalPlan) -> Result<bool> {
    match expr {
        Expr::Alias(alias) => projected_null(&alias.expr, input),
        Expr::Cast(cast) => projected_null(&cast.expr, input),
        Expr::TryCast(cast) => projected_null(&cast.expr, input),
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
            if f.func.inner().is::<SparkCheckedArithmetic>()
                || f.func.inner().is::<SparkCheckedDivide>()
                || f.func.inner().is::<SparkCheckedModulo>()
                || f.func.inner().is::<FusedDecimalDivide>())
    ))
}

impl AnalyzerRule for PreserveSparkExpressionErrors {
    fn name(&self) -> &str {
        "preserve_spark_expression_errors"
    }

    fn analyze(&self, plan: LogicalPlan, options: &ConfigOptions) -> Result<LogicalPlan> {
        // Borrow first: ordinary queries require no rewritten plan or batch allocation.
        let mut found = false;
        let mut check_arithmetic = false;
        let mut check_cast = false;
        let mut check_groups = false;
        let mut has_array_contains = false;
        let mut has_groups = false;
        plan.apply_with_subqueries(|node| {
            if let LogicalPlan::Aggregate(aggregate) = node
                && !aggregate
                    .group_expr
                    .iter()
                    .any(|expr| matches!(expr, Expr::GroupingSet(_)))
            {
                has_groups |= !aggregate.group_expr.is_empty();
                for expr in &aggregate.group_expr {
                    check_groups |= row_expression(expr, false)?;
                }
            }
            node.apply_expressions(|expr| {
                found |= expr.exists(|expr| Ok(candidate(expr)))?;
                check_cast |= has_cast(expr)?;
                check_arithmetic |= arithmetic_candidate(expr, node.schema())?;
                has_array_contains |= expr.exists(|expr| {
                    Ok(matches!(expr, Expr::ScalarFunction(f) if f.func.inner().is::<ArrayContains>()))
                })?;
                Ok(TreeNodeRecursion::Continue)
            })?;
            Ok(if found && check_arithmetic && check_cast && check_groups && has_array_contains {
                TreeNodeRecursion::Stop
            } else {
                TreeNodeRecursion::Continue
            })
        })?;
        if !found && !check_arithmetic && !check_cast && !check_groups && !has_array_contains {
            return Ok(plan);
        }
        let mut props = ExecutionProps::new();
        props.config_options = Some(Arc::new(options.clone()));
        let prepare = Prepare {
            props,
            has_groups,
            has_array_contains,
        };
        let plan = if check_groups {
            plan.transform_up_with_subqueries(prepare_foldable_groups)?
                .data
        } else {
            plan
        };
        let plan = if check_cast || check_arithmetic {
            plan.transform_down_up_with_subqueries(|node| {
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
                Ok(Transformed::no(node))
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
                                Expr::Exists(exists) => {
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
                                if check_arithmetic {
                                    prepare.check_arithmetic_expressions(&subquery.subquery, true)?;
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
                                if check_arithmetic {
                                    prepare.check_arithmetic_plan(&prepared, options, true)?;
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
                            if check_cast && let Expr::ScalarSubquery(mut subquery) = expr {
                                if let Some(mut expressions) = one_row_exprs(&subquery.subquery)?
                                    && expressions.len() == 1
                                    && row_expression(&expressions[0], true)?
                                {
                                    return Ok(Transformed::yes(expressions.remove(0)));
                                }
                                // Preserve local arithmetic errors before CAST
                                // preparation removes unused or empty projections.
                                if check_arithmetic {
                                    prepare.check_arithmetic_plan(&subquery.subquery, options, false)?;
                                }
                                subquery.subquery = Arc::new(prepare.cast_reachability(
                                    Arc::unwrap_or_clone(subquery.subquery),
                                    options,
                                    false,
                                )?);
                                return Ok(Transformed::yes(Expr::ScalarSubquery(subquery)));
                            }
                            if check_cast {
                                guard_cast_arithmetic(expr, &schema)
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
        if check_arithmetic {
            // CAST preparation already checked these scalar plans before pruning.
            if !check_cast {
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
                                prepare.check_arithmetic_plan(
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
            prepare.check_arithmetic_plan(&plan, options, false)?;
        }

        let plan = if check_cast {
            prepare.cast_reachability(plan, options, false)?
        } else if check_arithmetic {
            let prepared = prepare.prepare_distinct_limits(plan, 0)?;
            if prepared.transformed || has_groups || has_array_contains {
                let config = OptimizerContext::new_with_config_options(Arc::new(options.clone()));
                prepare.prune_projections(prepared.data, &config, false)?
            } else {
                prepared.data
            }
        } else if has_array_contains {
            let config = OptimizerContext::new_with_config_options(Arc::new(options.clone()));
            prepare.prune_projections(plan, &config, false)?
        } else {
            plan
        };
        if !found {
            return Ok(plan);
        }
        Ok(plan
            .transform_down_with_subqueries(|node| {
                let schema = Arc::new(datafusion_expr::utils::merge_schema(&node.inputs()));
                node.map_expressions(|expr| {
                    expr.transform_down(|expr| {
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
                    })
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

// Keep foldable outputs available for live uses, but do not evaluate them
// merely to form groups. A real key column preserves zero groups on empty input.
fn prepare_foldable_groups(plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
    use datafusion_expr::{Aggregate, Projection};
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
        .map(|expr| row_expression(expr, false))
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
    has_array_contains: bool,
}

// Physical row expressions only; aggregates, subqueries and outer references
// require their own planning context. Volatile calls belong to execution, even
// when their column inputs are already available as a local batch.
fn row_expression(expr: &Expr, columns: bool) -> Result<bool> {
    Ok(!expr.exists(|node| {
        Ok(match node {
            Expr::Column(_) => !columns,
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
            Expr::ScalarFunction(_) => node.is_volatile_node(),
            _ => true,
        })
    })?)
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
        || matches!(e, Expr::ScalarFunction(f) if f.func.inner().is::<sail_function::scalar::spark_cast_string_to_float::SparkCastStringToFloat>()
            || f.func.inner().is::<SparkCastStringToDecimal>())))
}

// Preserve string and Decimal-to-Decimal failures before local pruning.
// Other numeric casts retain the existing arithmetic precheck.
fn early_cast(expr: &Expr, schema: &DFSchemaRef) -> Result<bool> {
    expr.exists(|expr| match expr {
        Expr::Cast(cast) => {
            if null_literal(&cast.expr) || !row_expression(&cast.expr, true)? {
                return Ok(false);
            }
            let source = cast.expr.get_type(schema)?;
            Ok(source.is_string()
                || matches!((source, cast.field.data_type()),
                    (DataType::Decimal128(_, _), DataType::Decimal128(_, _))))
        }
        Expr::ScalarFunction(f)
            if f.func
                .inner()
                .is::<sail_function::scalar::spark_cast_string_to_float::SparkCastStringToFloat>(
            ) || f.func.inner().is::<SparkCastStringToDecimal>() =>
        {
            Ok(matches!(f.args.as_slice(), [expr] if !null_literal(expr) && row_expression(expr, true)?))
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
    fn simplify_projected_null_array_membership(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
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
                            && f.func.inner().is::<ArrayContains>()
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
                .transform_data(prepare_foldable_groups)
            })?
            .data)
    }

    fn prune_projections(
        &self,
        plan: LogicalPlan,
        config: &OptimizerContext,
        exists: bool,
    ) -> Result<LogicalPlan> {
        let plan = if self.has_array_contains {
            self.simplify_projected_null_array_membership(plan)?
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

    // Spark pulls correlations out after BooleanSimplification, before
    // ConstantFolding. Folding 1=2 or CAST(NULL AS BOOLEAN) here is too early.
    fn correlated_exists_uses_inner_columns(&self, plan: &LogicalPlan) -> Result<bool> {
        plan.exists(|node| {
            let LogicalPlan::Filter(filter) = node else {
                return Ok(false);
            };
            // ponytail: literal Boolean reductions only; extend this guard when
            // another Spark BooleanSimplification boundary is reproduced.
            let predicate = filter
                .predicate
                .clone()
                .transform_up(|expr| {
                    use Expr::Literal;
                    use Operator::{And, Or};
                    use ScalarValue::Boolean;
                    let reduced = match &expr {
                        Expr::BinaryExpr(binary) => {
                            match (&*binary.left, binary.op, &*binary.right) {
                                (Literal(Boolean(Some(false)), _), And, _)
                                | (_, And, Literal(Boolean(Some(false)), _)) => Some(lit(false)),
                                (Literal(Boolean(Some(true)), _), Or, _)
                                | (_, Or, Literal(Boolean(Some(true)), _)) => Some(lit(true)),
                                (Literal(Boolean(Some(true)), _), And, right)
                                | (Literal(Boolean(Some(false)), _), Or, right) => {
                                    Some(right.clone())
                                }
                                (left, And, Literal(Boolean(Some(true)), _))
                                | (left, Or, Literal(Boolean(Some(false)), _)) => {
                                    Some(left.clone())
                                }
                                _ => None,
                            }
                        }
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
                })?
                .data;
            Ok(split_conjunction_owned(predicate)
                .iter()
                .any(|expr| expr.contains_outer() && expr.any_column_refs()))
        })
    }

    fn cast_reachability(
        &self,
        plan: LogicalPlan,
        options: &ConfigOptions,
        exists: bool,
    ) -> Result<LogicalPlan> {
        use datafusion_expr::logical_plan::EmptyRelation;
        // Local VALUES conversion precedes pruning. Empty local inputs never
        // evaluate their projection; nonempty ones preserve live CAST failures.
        let plan = plan
            .transform_up(|node| {
                if let LogicalPlan::Projection(project) = &node {
                    let mut casts = false;
                    let mut evaluable = true;
                    for expr in &project.expr {
                        casts |= early_cast(expr, project.input.schema())?;
                        evaluable &= row_expression(expr, true)?;
                    }
                    if casts
                        && evaluable
                        && let Some(batch) = self.local_batch(&project.input)?
                    {
                        if batch.num_rows() == 0 {
                            return Ok(Transformed::yes(LogicalPlan::EmptyRelation(
                                EmptyRelation {
                                    produce_one_row: false,
                                    schema: Arc::clone(&project.schema),
                                },
                            )));
                        }
                        let info = datafusion_expr::simplify::SimplifyContext::builder()
                            .with_schema(Arc::clone(project.input.schema()))
                            .with_config_options(Arc::new(options.clone()))
                            .build();
                        let simplifier =
                            datafusion::optimizer::simplify_expressions::ExprSimplifier::new(info);
                        for expr in &project.expr {
                            // Lower conditional UDFs such as COALESCE before physical
                            // evaluation; their dead branches still cannot fail folding.
                            let expr = simplifier.simplify(expr.clone())?;
                            create_physical_expr(&expr, project.input.schema(), &self.props)?
                                .evaluate(&batch)?;
                        }
                    }
                }
                Ok(Transformed::no(node))
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
        let plan = self.prune_projections(plan, &config, exists)?;
        Ok(plan
            .transform_down(|node| {
                if let LogicalPlan::Limit(limit) = &node
                    && self.limit_count(limit.fetch.as_deref(), usize::MAX)? == Some(0)
                {
                    if known_empty(&limit.input) {
                        return Ok(Transformed::yes(Arc::unwrap_or_clone(limit.input.clone())));
                    }
                    return Ok(Transformed::yes(LogicalPlan::EmptyRelation(
                        EmptyRelation {
                            produce_one_row: false,
                            schema: Arc::clone(limit.input.schema()),
                        },
                    )));
                }
                Ok(Transformed::no(node))
            })?
            .data)
    }

    fn eval_constant(&self, expr: &Expr) -> Result<ColumnarValue> {
        let batch = RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(1)),
        )?;
        create_physical_expr(expr, &DFSchema::empty(), &self.props)?.evaluate(&batch)
    }

    fn limit_count(&self, expr: Option<&Expr>, default: usize) -> Result<Option<usize>> {
        let Some(expr) = expr else {
            return Ok(Some(default));
        };
        if !row_expression(expr, false)? {
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
        let info = datafusion_expr::simplify::SimplifyContext::builder()
            .with_schema(Arc::clone(schema))
            .with_config_options(Arc::clone(self.props.config_options.as_ref().unwrap()))
            .build();
        Ok(expr
            .transform_down(|expr| {
                let expr = if local {
                    let right = match &expr {
                        Expr::BinaryExpr(binary)
                            if matches!(binary.op, Operator::Divide | Operator::Modulo) =>
                        {
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
                        return Ok(Transformed::yes(lit(ScalarValue::try_new_null(
                            &expr.get_type(schema)?,
                        )?)));
                    }
                    expr
                } else if null_round(&expr) && !expr.contains_scalar_subquery() {
                    lit(ScalarValue::try_new_null(&expr.get_type(schema)?)?)
                } else {
                    prune_null(expr, &info)?.data
                };
                let expr = if let Expr::ScalarFunction(f) = &expr
                    && f.func.short_circuits()
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
                // Dead conditional parents can make a derived DIV column unused.
                // Do not simplify arithmetic or Boolean parents before checking it.
                if !local
                    && matches!(expr, Expr::Case(_))
                    && !arithmetic_candidate(&expr, schema)?
                    && let Ok(simplified) = ExprSimplifier::new(info.clone()).simplify(expr.clone())
                {
                    return Ok(Transformed::yes(simplified));
                }
                Ok(Transformed::no(expr))
            })?
            .data)
    }

    fn check_arithmetic_plan(
        &self,
        plan: &LogicalPlan,
        options: &ConfigOptions,
        limits_prepared: bool,
    ) -> Result<()> {
        // Local VALUES are evaluated before column pruning and LIMIT 0.
        // Successful constants need no extra copy, simplification or pruning.
        if self.check_arithmetic_expressions(plan, true)? {
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
            self.check_arithmetic_expressions(&pruned, false)?;
        }
        Ok(())
    }

    fn check_arithmetic_expressions(&self, plan: &LogicalPlan, local: bool) -> Result<bool> {
        let mut needs_pruning = false;
        plan.apply(|node| {
            if !local
                && let LogicalPlan::Limit(limit) = node
                && self.limit_count(limit.fetch.as_deref(), usize::MAX)? == Some(0)
                && !known_empty(&limit.input)
            {
                return Ok(TreeNodeRecursion::Jump);
            }
            let schema = Arc::new(datafusion_expr::utils::merge_schema(&node.inputs()));
            node.apply_expressions(|expr| {
                if !arithmetic_candidate(expr, &schema)? {
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
                match batch {
                    Some(batch) if local && batch.num_rows() > 0 && row_expression(expr, true)? => {
                        let expr = self.precheck_expression(expr.clone(), &schema, true)?;
                        let error = match create_physical_expr(&expr, &schema, &self.props)?
                            .evaluate(&batch)
                        {
                            Ok(_) => return Ok(TreeNodeRecursion::Continue),
                            Err(error) => error,
                        };
                        // Match Spark's strict string-to-integer trimming
                        // only for this discarded local evaluation. The
                        // runtime expression and its physical plan stay intact.
                        let expr = expr.clone().transform_up(|expr| {
                            if let Expr::Cast(mut cast) = expr {
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
                                // Spark compares its floating input with a
                                // floating Long.MaxValue, which rounds to
                                // 2^63, then saturates that endpoint to MAX.
                                if cast.field.data_type() == &DataType::Int64 {
                                    let upper = match cast.expr.get_type(&schema)? {
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
                                            (*cast.expr).clone().eq(lit(upper)),
                                            lit(i64::MAX),
                                        )
                                        .otherwise(Expr::Cast(cast))?;
                                        return Ok(Transformed::yes(expr));
                                    }
                                }
                                Ok(Transformed::no(Expr::Cast(cast)))
                            } else {
                                Ok(Transformed::no(expr))
                            }
                        })?;
                        if !expr.transformed {
                            return Err(error);
                        }
                        create_physical_expr(&expr.data, &schema, &self.props)?.evaluate(&batch)?;
                    }
                    None => {
                        let expr = self.precheck_expression(expr.clone(), &schema, false)?;
                        if local {
                            needs_pruning |= self.check_expr(&expr, true).is_err();
                        } else {
                            self.check_expr(&expr, true)?;
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
        if constants && row_expression(expr, false)? {
            self.eval_constant(expr)?;
            return Ok(());
        }
        // Spark defers failed constant evaluation inside nonconstant CASE and
        // other conditional expressions. Subqueries still undergo optimization.
        let conditional = matches!(expr, Expr::Case(_))
            || matches!(expr, Expr::ScalarFunction(f) if f.func.short_circuits());
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
    // The batch is discarded; it exists to preserve early planning failures.
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
                    Ok::<_, datafusion_common::DataFusionError>(valid && row_expression(e, false)?)
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
                    Ok::<_, datafusion_common::DataFusionError>(valid && row_expression(e, true)?)
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
                if !row_expression(&predicate, true)? {
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

#[tokio::test]
async fn integer_divide_evaluation_order()
-> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use crate::{config::PlanConfig, physical_plan::SparkQueryPlanner, resolver::PlanResolver};
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::SessionContext;

    let ctx = SessionContext::new_with_state(
        SessionStateBuilder::new()
            .with_default_features()
            .with_analyzer_rule(Arc::new(PreserveSparkExpressionErrors))
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
            .with_analyzer_rule(Arc::new(PreserveSparkExpressionErrors))
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
            .with_analyzer_rule(Arc::new(PreserveSparkExpressionErrors))
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
            .with_analyzer_rule(Arc::new(PreserveSparkExpressionErrors))
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
            .with_analyzer_rule(Arc::new(PreserveSparkExpressionErrors))
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
            .with_analyzer_rule(Arc::new(PreserveSparkExpressionErrors))
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
                let result =
                    async { ctx.execute_logical_plan(named.plan).await?.collect().await }.await;
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
            .with_analyzer_rule(Arc::new(PreserveSparkExpressionErrors))
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
            .with_analyzer_rule(Arc::new(PreserveSparkExpressionErrors))
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
                    ctx.execute_logical_plan(named.plan)
                        .await?
                        .collect()
                        .await?,
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
            .with_analyzer_rule(Arc::new(PreserveSparkExpressionErrors))
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
                    ctx.execute_logical_plan(named.plan)
                        .await?
                        .collect()
                        .await?,
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
            .with_analyzer_rule(Arc::new(PreserveSparkExpressionErrors))
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
                    ctx.execute_logical_plan(named.plan)
                        .await?
                        .collect()
                        .await?,
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
            .with_analyzer_rule(Arc::new(PreserveSparkExpressionErrors))
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
                    ctx.execute_logical_plan(named.plan)
                        .await?
                        .collect()
                        .await?,
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
            .with_analyzer_rule(Arc::new(PreserveSparkExpressionErrors))
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
                    ctx.execute_logical_plan(named.plan)
                        .await?
                        .collect()
                        .await?,
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
            .with_analyzer_rule(Arc::new(PreserveSparkExpressionErrors))
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
                    ctx.execute_logical_plan(named.plan)
                        .await?
                        .collect()
                        .await?,
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
            .with_analyzer_rule(Arc::new(PreserveSparkExpressionErrors))
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
                    let result =
                        async { ctx.execute_logical_plan(named.plan).await?.collect().await }.await;
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
            .with_analyzer_rule(Arc::new(PreserveSparkExpressionErrors))
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
                    let result =
                        async { ctx.execute_logical_plan(named.plan).await?.collect().await }.await;
                    if ansi && fails {
                        let error = result.unwrap_err().to_string().to_lowercase();
                        let cause = if expression.starts_with("CAST") {
                            "cast"
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
            .with_analyzer_rule(Arc::new(PreserveSparkExpressionErrors))
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
                    let result =
                        async { ctx.execute_logical_plan(named.plan).await?.collect().await }.await;
                    if ansi && fails {
                        let error = result.unwrap_err().to_string().to_lowercase();
                        let cause = if expression.starts_with("CAST") {
                            "cast"
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
            .with_analyzer_rule(Arc::new(PreserveSparkExpressionErrors))
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
                    let result =
                        async { ctx.execute_logical_plan(named.plan).await?.collect().await }.await;
                    if ansi && fails {
                        let error = result.unwrap_err().to_string().to_lowercase();
                        let cause = if expression.starts_with("CAST") {
                            "cast"
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
            let batches = ctx
                .execute_logical_plan(named.plan)
                .await?
                .collect()
                .await?;
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
            .with_analyzer_rule(Arc::new(PreserveSparkExpressionErrors))
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
                        let result =
                            async { ctx.execute_logical_plan(named.plan).await?.collect().await }
                                .await;
                        if ansi && fails {
                            let error = result.unwrap_err().to_string().to_lowercase();
                            let cause = if expression.starts_with("CAST") {
                                "cast"
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
            let physical = ctx
                .execute_logical_plan(named.plan)
                .await?
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
                .with_analyzer_rule(Arc::new(PreserveSparkExpressionErrors))
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
