// Local Spark LIKE initialization policy. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md.
use std::{any::Any, sync::Arc};

use chrono::{DateTime, Utc};
use datafusion::execution::SessionState;
use datafusion::logical_expr::execution_props::ExecutionProps;
use datafusion::optimizer::eliminate_filter::EliminateFilter;
use datafusion::optimizer::eliminate_limit::EliminateLimit;
use datafusion::optimizer::optimize_projections::OptimizeProjections;
use datafusion::optimizer::propagate_empty_relation::PropagateEmptyRelation;
use datafusion::optimizer::simplify_expressions::SimplifyExpressions;
use datafusion::optimizer::{AnalyzerRule, Optimizer, OptimizerContext};
use datafusion_common::config::{ConfigEntry, ConfigExtension, ConfigOptions, ExtensionOptions};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{Result, config_err};
use datafusion_expr::{Expr, LogicalPlan, ScalarUDF};
use sail_function::scalar::predicate::spark_like::SparkLike;

use crate::expression_errors::{
    SparkSqlExpressionSemantics, early_local_relation, prune_retained_local_like_limits,
    try_check_local_row_errors,
};

/// Spark initializes foldable LIKE patterns even on empty or all-NULL providers.
/// Keep native NULL/CASE/empty-input pruning before that initialization check.
#[derive(Debug)]
pub struct PreserveSparkLikeErrors;

// AnalyzerRule receives options but no execution properties. Keep the actual
// query clock in its cloned state without changing shared Spark declarations.
#[derive(Clone, Debug)]
struct SparkLikeQueryClock(Option<DateTime<Utc>>);

impl ConfigExtension for SparkLikeQueryClock {
    const PREFIX: &'static str = "dar_spark_like_query_clock";
}

impl ExtensionOptions for SparkLikeQueryClock {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, key: &str, _value: &str) -> Result<()> {
        config_err!("Spark LIKE query clock has no configurable option '{key}'")
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        vec![]
    }
}

/// Retain this query state's execution clock for mandatory LIKE preparation.
pub fn set_spark_like_query_clock(state: &mut SessionState) {
    let clock = SparkLikeQueryClock(state.execution_props().query_execution_start_time);
    state.config_mut().options_mut().extensions.insert(clock);
}

pub(crate) fn contains_spark_like(plan: &LogicalPlan) -> Result<bool> {
    let mut found = false;
    plan.apply_with_subqueries(|node| {
        node.apply_expressions(|expr| {
            found |= expr.exists(|expr| {
                Ok(matches!(expr, Expr::ScalarFunction(f) if f.func.inner().is::<SparkLike>()))
            })?;
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(if found {
            TreeNodeRecursion::Stop
        } else {
            TreeNodeRecursion::Continue
        })
    })?;
    Ok(found)
}

pub(crate) fn contains_early_local_like(plan: &LogicalPlan) -> Result<bool> {
    plan.exists(|node| {
        if !matches!(node, LogicalPlan::Projection(_) | LogicalPlan::Filter(_))
            || !early_local_relation(node)?
        {
            return Ok(false);
        }
        let mut found = false;
        node.apply_expressions(|expr| {
            found |= expr.exists(|expr| {
                Ok(matches!(expr, Expr::ScalarFunction(f) if f.func.inner().is::<SparkLike>()))
            })?;
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(found)
    })
}

fn contains_offset(plan: &LogicalPlan) -> Result<bool> {
    plan.exists(|node| Ok(matches!(node, LogicalPlan::Limit(limit) if limit.skip.is_some())))
}

// Spark's early local conversion has no OFFSET arm. Later deterministic
// projections move below surviving LIMIT/OFFSET operators. Stop as soon as a
// subtree could already have been consumed early, retaining its LIMIT slicing.
fn before_late_slices(plan: LogicalPlan) -> Result<LogicalPlan> {
    if !contains_offset(&plan)? {
        return Ok(plan);
    }
    Ok(match plan {
        LogicalPlan::Limit(limit) => before_late_slices(Arc::unwrap_or_clone(limit.input))?,
        LogicalPlan::SubqueryAlias(mut alias) => {
            alias.input = Arc::new(before_late_slices(Arc::unwrap_or_clone(alias.input))?);
            LogicalPlan::SubqueryAlias(alias)
        }
        LogicalPlan::Projection(mut project)
            if project.expr.iter().all(|expr| !expr.is_volatile()) =>
        {
            project.input = Arc::new(before_late_slices(Arc::unwrap_or_clone(project.input))?);
            LogicalPlan::Projection(project)
        }
        plan => plan,
    })
}

fn consume_local_likes(
    plan: LogicalPlan,
    props: &ExecutionProps,
    after_pruning: bool,
) -> Result<Transformed<LogicalPlan>> {
    plan.transform_up_with_subqueries(|node| {
        let input = match &node {
            LogicalPlan::Projection(project) => {
                if after_pruning && project.expr.iter().all(|expr| !expr.is_volatile()) {
                    before_late_slices(project.input.as_ref().clone())?
                } else {
                    project.input.as_ref().clone()
                }
            }
            LogicalPlan::Filter(filter) => filter.input.as_ref().clone(),
            _ => return Ok(Transformed::no(node)),
        };
        if contains_offset(&input)? {
            return Ok(Transformed::no(node));
        }
        // Spark rejects the whole Project when any sibling is unevaluable.
        // Structural eligibility allows volatile siblings; only the pure
        // LIKE-containing expression is evaluated below.
        let mut local = node.clone();
        match &mut local {
            LogicalPlan::Projection(project) => project.input = Arc::new(input.clone()),
            LogicalPlan::Filter(filter) => filter.input = Arc::new(input.clone()),
            _ => unreachable!(),
        }
        if !early_local_relation(&local)? {
            return Ok(Transformed::no(node));
        }
        node.map_expressions(|expr| {
            let has_like = expr.exists(|expr| {
                Ok(matches!(expr, Expr::ScalarFunction(f)
                        if f.func.inner().downcast_ref::<SparkLike>()
                            .is_some_and(SparkLike::requires_literal_initialization)))
            })?;
            if !has_like || !try_check_local_row_errors(&expr, &input, props)? {
                return Ok(Transformed::no(expr));
            }
            expr.transform_down(|mut expr| {
                if let Expr::ScalarFunction(function) = &mut expr
                    && let Some(like) = function.func.inner().downcast_ref::<SparkLike>()
                {
                    function.func = Arc::new(ScalarUDF::from(like.after_local_evaluation()));
                    Ok(Transformed::yes(expr))
                } else {
                    Ok(Transformed::no(expr))
                }
            })
        })
    })
}

impl AnalyzerRule for PreserveSparkLikeErrors {
    fn name(&self) -> &str {
        "preserve_spark_like_errors"
    }

    fn analyze(&self, plan: LogicalPlan, options: &ConfigOptions) -> Result<LogicalPlan> {
        if options
            .extensions
            .get::<SparkSqlExpressionSemantics>()
            .is_none()
        {
            return Ok(plan);
        }
        if !contains_spark_like(&plan)? {
            return Ok(plan);
        }
        // Spark interprets local VALUES projections before parent LIMIT pruning
        // and before generated-code literal initialization. Reuse the existing
        // pure row evaluator; preserve its errors without materializing results.
        let mut props = ExecutionProps::new();
        props.config_options = Some(Arc::new(options.clone()));
        props.query_execution_start_time = options
            .extensions
            .get::<SparkLikeQueryClock>()
            .and_then(|clock| clock.0);
        let local = consume_local_likes(plan, &props, false)?;
        // Keep early local-consumption marks in the real plan; later checks
        // and marks belong only to the discarded reachability copy.
        let output = local.data.clone();
        // Further pruning and late checks use a discarded reachability copy.
        let optimizer = Optimizer::with_rules(vec![
            Arc::new(EliminateLimit::new()),
            Arc::new(SimplifyExpressions::new()),
            Arc::new(EliminateFilter::new()),
            Arc::new(PropagateEmptyRelation::new()),
            Arc::new(OptimizeProjections::new()),
        ]);
        // Reachability is required semantics, independent of optional host
        // optimization. Change only this discarded copy's execution controls.
        let mut preview_options = options.clone();
        preview_options.optimizer.skip_failed_rules = false;
        preview_options.optimizer.max_passes = ConfigOptions::default().optimizer.max_passes;
        let mut config = OptimizerContext::new_with_config_options(Arc::new(preview_options))
            .without_query_execution_start_time();
        if let Some(clock) = props.query_execution_start_time {
            config = config.with_query_execution_start_time(clock);
        }
        let prepared = optimizer.optimize(local.data, &config, |_, _| {})?;
        let prepared = consume_local_likes(prepared, &props, true)?.data;
        prepared.apply_with_subqueries(|node| {
            node.apply_expressions(|expr| {
                expr.apply(|expr| {
                    if let Expr::ScalarFunction(f) = expr
                        && let Some(function) = f.func.inner().downcast_ref::<SparkLike>()
                        && function.requires_literal_initialization()
                        && let Some(Expr::Literal(value, _)) = f.args.get(1)
                        && let Some(Some(pattern)) = value.try_as_str()
                    {
                        function.validate_pattern(pattern)?;
                    }
                    Ok(TreeNodeRecursion::Continue)
                })
            })?;
            Ok(TreeNodeRecursion::Continue)
        })?;
        prune_retained_local_like_limits(output, &props)
    }
}
