use std::sync::Arc;

use datafusion::optimizer::decorrelate::PullUpCorrelatedExpr;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, DFSchema, Result};
use datafusion_expr::expr_rewriter::strip_outer_reference;
use datafusion_expr::utils::{conjunction, split_conjunction};
use datafusion_expr::{
    Aggregate, Distinct, Expr, Limit, LogicalPlan, LogicalPlanBuilder, Operator, Projection, Sort,
    SubqueryAlias, lit, when,
};

use super::aggregate::{aggregate_inner_rows, can_lower_aggregate, distinct_inner_rows};
use super::filter_match_marker;
use super::limit::{can_lower_limit, limit_inner_rows};

pub(super) struct CorrelatedRows<'a> {
    pub input: LogicalPlan,
    pub join_filter: Expr,
    pub steps: Vec<RowStep<'a>>,
}

pub(super) enum RowStep<'a> {
    Project(&'a Projection),
    Alias(&'a SubqueryAlias),
    Filter(&'a Expr),
    Aggregate(&'a Aggregate),
    Distinct(&'a Arc<LogicalPlan>),
    Limit(&'a Limit, Option<&'a Sort>),
}

pub(super) fn split_correlated_rows(mut input: &LogicalPlan) -> Result<Option<CorrelatedRows<'_>>> {
    let mut steps = Vec::new();
    loop {
        if can_pull_up(input)? {
            let mut pull_up = PullUpCorrelatedExpr::new();
            let inner = preserve_self_equalities(input.clone())?
                .rewrite(&mut pull_up)
                .data()?;
            if pull_up.can_pull_up {
                // Correlation can appear only in the selected value.
                let join_filter = conjunction(pull_up.join_filters).unwrap_or_else(|| lit(true));
                return Ok(Some(CorrelatedRows { input: inner, join_filter, steps }));
            }
        }
        // Keep row-changing operations on the per-outer-row side of the join.
        input = match input {
            LogicalPlan::Projection(project) => {
                steps.push(RowStep::Project(project));
                &project.input
            }
            LogicalPlan::SubqueryAlias(alias) => {
                steps.push(RowStep::Alias(alias));
                &alias.input
            }
            LogicalPlan::Filter(filter) => {
                steps.push(RowStep::Filter(&filter.predicate));
                &filter.input
            }
            LogicalPlan::Aggregate(aggregate) if can_lower_aggregate(aggregate) => {
                steps.push(RowStep::Aggregate(aggregate));
                &aggregate.input
            }
            LogicalPlan::Distinct(Distinct::All(input)) if !input.schema().fields().is_empty() => {
                steps.push(RowStep::Distinct(input));
                input
            }
            LogicalPlan::Limit(limit) if can_lower_limit(limit) => {
                split_limit(limit, &mut steps)
            }
            _ => return Ok(None),
        };
    }
}

fn can_pull_up(plan: &LogicalPlan) -> Result<bool> {
    let mut supported = true;
    plan.apply(|node| {
        Ok(match node {
            LogicalPlan::Filter(_) | LogicalPlan::SubqueryAlias(_) => TreeNodeRecursion::Continue,
            LogicalPlan::Projection(_) if !node.contains_outer_reference() => {
                TreeNodeRecursion::Continue
            }
            // Uncorrelated subtrees keep their original operators, including joins.
            node if node.all_out_ref_exprs().is_empty() => TreeNodeRecursion::Jump,
            _ => {
                supported = false;
                TreeNodeRecursion::Stop
            }
        })
    })?;
    Ok(supported)
}

fn split_limit<'a>(limit: &'a Limit, steps: &mut Vec<RowStep<'a>>) -> &'a LogicalPlan {
    let mut input = limit.input.as_ref();
    let mut wrappers = Vec::new();
    while let LogicalPlan::Projection(projection) = input {
        // A projection can hide an ORDER BY key from the scalar result.
        wrappers.push(RowStep::Project(projection));
        input = &projection.input;
    }
    if let LogicalPlan::Sort(sort) = input {
        steps.extend(wrappers);
        steps.push(RowStep::Limit(limit, Some(sort)));
        &sort.input
    } else {
        steps.push(RowStep::Limit(limit, None));
        &limit.input
    }
}

fn is_correlated_self_equality(expr: &Expr) -> bool {
    matches!(expr, Expr::BinaryExpr(binary)
        if binary.op == Operator::Eq && binary.left == binary.right && expr.contains_outer())
}

pub(super) fn has_correlated_self_equality(plan: &LogicalPlan) -> Result<bool> {
    plan.exists(|node| {
        Ok(matches!(node, LogicalPlan::Filter(filter)
            if split_conjunction(&filter.predicate).into_iter().any(is_correlated_self_equality)))
    })
}

fn preserve_self_equalities(input: LogicalPlan) -> Result<LogicalPlan> {
    input.transform_up(|node| {
        let LogicalPlan::Filter(mut filter) = node else {
            return Ok(Transformed::no(node));
        };
        let mut changed = false;
        let predicates = split_conjunction(&filter.predicate)
            .into_iter()
            .map(|predicate| {
                if is_correlated_self_equality(predicate) {
                    // Native pull-up discards bare self-equality even on NULL.
                    changed = true;
                    Expr::IsTrue(Box::new(predicate.clone()))
                } else {
                    predicate.clone()
                }
            })
            .collect::<Vec<_>>();
        if changed && let Some(predicate) = conjunction(predicates) {
            filter.predicate = predicate;
        }
        Ok(Transformed::new_transformed(LogicalPlan::Filter(filter), changed))
    }).data()
}

pub(super) fn apply_row_steps(
    mut plan: LogicalPlan,
    steps: Vec<RowStep<'_>>,
    row_id: &Column,
    marker: &Column,
    outer_columns: &[Column],
) -> Result<LogicalPlan> {
    for step in steps.into_iter().rev() {
        plan = match step {
            RowStep::Project(project) => project_inner_rows(
                plan, project.expr.iter().cloned(), &project.schema, row_id, marker, outer_columns,
            )?,
            RowStep::Alias(alias) => project_inner_rows(
                plan,
                alias.input.schema().columns().into_iter().map(Expr::Column),
                &alias.schema,
                row_id,
                marker,
                outer_columns,
            )?,
            RowStep::Filter(predicate) => {
                filter_match_marker(plan, marker, strip_outer_reference(predicate.clone()))?
            }
            RowStep::Aggregate(aggregate) => {
                aggregate_inner_rows(plan, aggregate, row_id, marker, outer_columns)?
            }
            RowStep::Distinct(input) => distinct_inner_rows(plan, input, row_id, marker, outer_columns)?,
            RowStep::Limit(limit, sort) => limit_inner_rows(plan, limit, sort, row_id, marker)?,
        };
    }
    Ok(plan)
}

fn project_inner_rows(
    plan: LogicalPlan,
    inner: impl IntoIterator<Item = Expr>,
    output: &DFSchema,
    row_id: &Column,
    marker: &Column,
    outer_columns: &[Column],
) -> Result<LogicalPlan> {
    let is_match = Expr::Column(marker.clone()).is_not_null();
    let mut expressions = Vec::new();
    for (expr, column) in inner.into_iter().zip(output.columns()) {
        let expr = strip_outer_reference(expr.unalias());
        let expr = match expr {
            Expr::Column(_) | Expr::Literal(_, _) => expr,
            expr => when(is_match.clone(), expr).end()?,
        };
        expressions.push(expr.alias_qualified(column.relation, column.name));
    }
    expressions.extend(outer_columns.iter().chain([row_id, marker]).cloned().map(Expr::Column));
    LogicalPlanBuilder::from(plan).project(expressions)?.build()
}
