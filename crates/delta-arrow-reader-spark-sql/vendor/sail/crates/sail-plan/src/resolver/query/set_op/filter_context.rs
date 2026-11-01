//! Follow enclosing filters through column references without expanding projections.
//! Reusing `a + b` in several projections must not copy its expression tree at each level.

use datafusion::execution::SessionState;
use std::collections::HashMap;

use datafusion::optimizer::eliminate_outer_join::{
    NullRejectionContext, eliminate_outer, rejects_null_input,
};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{JoinType, Result};
use datafusion_expr::{Distinct, Expr, Join, LogicalPlan};
use sail_logical_plan::sort::SortWithinPartitionsNode;

use super::constant_columns::fold_immutable_expression;
use super::pending::PendingSetOperation;
use crate::function::common::SparkComparisonFloat;

#[derive(Default)]
pub(super) struct EnclosingQueryContext<'a> {
    path: Vec<(&'a LogicalPlan, usize)>,
    conditions: Vec<(usize, FilterCondition<'a>)>,
}

enum FilterCondition<'a> {
    Predicate(&'a Expr),
    NonNullColumn(usize),
}

impl<'a> EnclosingQueryContext<'a> {
    pub(super) fn has_single_row_limit(&self) -> Result<bool> {
        use datafusion_common::ScalarValue;
        let Some((root, _)) = self.path.first() else {
            return Ok(false);
        };
        let mut required = vec![true; root.schema().fields().len()];
        let mut limited_to_one = false;
        for (plan, input_index) in &self.path {
            let input = plan.inputs()[*input_index];
            match plan {
                LogicalPlan::Projection(projection) => {
                    let mut input_required = vec![false; input.schema().fields().len()];
                    for (expr, required) in projection.expr.iter().zip(&required) {
                        if !required {
                            continue;
                        }
                        if has_subquery_or_outer_reference(expr)? {
                            limited_to_one = false;
                            input_required.fill(true);
                            break;
                        }
                        // A retained volatile output can prevent Spark's group-only DISTINCT removal.
                        limited_to_one &= !expr.is_volatile();
                        for column in expr.column_refs() {
                            input_required[input.schema().index_of_column(column)?] = true;
                        }
                    }
                    required = input_required;
                }
                LogicalPlan::SubqueryAlias(_) => {}
                LogicalPlan::Filter(filter) => {
                    limited_to_one = false;
                    if has_subquery_or_outer_reference(&filter.predicate)? {
                        required.fill(true);
                        continue;
                    }
                    for column in filter.predicate.column_refs() {
                        required[input.schema().index_of_column(column)?] = true;
                    }
                }
                LogicalPlan::Limit(limit) => {
                    limited_to_one = matches!(
                        limit.fetch.as_deref(),
                        Some(Expr::Literal(
                            ScalarValue::Int32(Some(1)) | ScalarValue::Int64(Some(1)),
                            _
                        ))
                    ) && matches!(
                        limit.skip.as_deref(),
                        None | Some(Expr::Literal(
                            ScalarValue::Int32(Some(0)) | ScalarValue::Int64(Some(0)),
                            _
                        ))
                    );
                }
                _ => {
                    limited_to_one = false;
                    required = vec![true; input.schema().fields().len()];
                }
            }
        }
        Ok(limited_to_one)
    }

    pub(super) fn enter_input(&mut self, plan: &'a LogicalPlan, input: usize) -> usize {
        let saved = self.conditions.len();
        self.path.push((plan, input));
        if let LogicalPlan::Filter(filter) = plan {
            self.conditions.push((
                self.path.len(),
                FilterCondition::Predicate(&filter.predicate),
            ));
        }
        saved
    }

    pub(super) fn require_non_null(&mut self, column: usize) {
        self.conditions
            .push((self.path.len(), FilterCondition::NonNullColumn(column)));
    }

    pub(super) fn leave_input(&mut self, saved_conditions: usize) {
        self.path.pop();
        self.conditions.truncate(saved_conditions);
    }

    pub(super) fn effective_join_type(
        &self,
        join: &Join,
        session: &SessionState,
    ) -> Result<JoinType> {
        if !join.join_type.is_outer() {
            return Ok(join.join_type);
        }
        let rejects_side = |right_side| -> Result<bool> {
            let mut lineage = NullSideLineage {
                path: &self.path,
                join,
                session,
                right_side,
                memo: HashMap::new(),
            };
            for (depth, condition) in &self.conditions {
                let rejects = match condition {
                    FilterCondition::Predicate(expr) => lineage.expression_rejects_null(
                        *depth,
                        expr,
                        NullRejectionContext::Predicate,
                    )?,
                    FilterCondition::NonNullColumn(index) => {
                        lineage.column_rejects_null(*depth, *index, NullRejectionContext::Value)?
                    }
                };
                if rejects {
                    return Ok(true);
                }
            }
            Ok(false)
        };
        Ok(eliminate_outer(
            join.join_type,
            rejects_side(false)?,
            rejects_side(true)?,
        ))
    }
}

fn has_subquery_or_outer_reference(expr: &Expr) -> Result<bool> {
    expr.exists(|expr| {
        Ok(matches!(
            expr,
            Expr::OuterReferenceColumn(..)
                | Expr::ScalarSubquery(_)
                | Expr::Exists(_)
                | Expr::InSubquery(_)
                | Expr::SetComparison(_)
        ))
    })
}

struct NullSideLineage<'a> {
    path: &'a [(&'a LogicalPlan, usize)],
    join: &'a Join,
    session: &'a SessionState,
    right_side: bool,
    memo: HashMap<(usize, usize, NullRejectionContext), bool>,
}

impl NullSideLineage<'_> {
    fn expression_rejects_null(
        &mut self,
        depth: usize,
        expr: &Expr,
        context: NullRejectionContext,
    ) -> Result<bool> {
        // Inspect one stored expression at a time; these wrappers preserve NULL at any depth.
        let structural_expr = expr
            .clone()
            .transform_up(|expr| {
                Ok(match expr {
                    Expr::Alias(alias) => Transformed::yes(*alias.expr),
                    Expr::ScalarFunction(mut function)
                        if function.func.inner().is::<SparkComparisonFloat>() =>
                    {
                        Transformed::yes(function.args.remove(0))
                    }
                    expr => Transformed::no(expr),
                })
            })?
            .data;
        if rejects_null_input(&structural_expr, context, &mut |column, context| {
            let schema = self
                .path
                .get(depth)
                .map_or(self.join.schema.as_ref(), |(plan, _)| plan.schema());
            self.column_rejects_null(depth, schema.index_of_column(column)?, context)
        })? {
            return Ok(true);
        }

        // Native folding also recognizes strict functions and NOT IS NULL before optimization.
        let schema = self
            .path
            .get(depth)
            .map_or(&self.join.schema, |(plan, _)| plan.schema())
            .clone();
        let mut replaced_null_input = false;
        let with_nulls = expr.clone().transform_up(|expr| {
            if let Expr::Alias(alias) = expr {
                return Ok(Transformed::yes(*alias.expr));
            }
            if let Expr::Column(column) = &expr {
                let index = schema.index_of_column(column)?;
                if self.column_rejects_null(depth, index, NullRejectionContext::Value)? {
                    replaced_null_input = true;
                    return Ok(Transformed::yes(Expr::Literal(
                        datafusion_common::ScalarValue::try_new_null(
                            schema.field(index).data_type(),
                        )?,
                        None,
                    )));
                }
            }
            Ok(Transformed::no(expr))
        });
        if !replaced_null_input {
            return Ok(false);
        }
        Ok(with_nulls
            .and_then(|result| fold_immutable_expression(result.data, schema, self.session))
            .is_ok_and(|expr| {
                matches!(expr, Expr::Literal(value, _)
                if value.is_null() || (context == NullRejectionContext::Predicate
                    && value == datafusion_common::ScalarValue::Boolean(Some(false))))
            }))
    }

    fn column_rejects_null(
        &mut self,
        depth: usize,
        index: usize,
        context: NullRejectionContext,
    ) -> Result<bool> {
        let key = (depth, index, context);
        if let Some(rejects) = self.memo.get(&key) {
            return Ok(*rejects);
        }
        let rejects = if let Some(&(plan, input)) = self.path.get(depth) {
            match plan {
                LogicalPlan::Projection(projection) => {
                    self.expression_rejects_null(depth + 1, &projection.expr[index], context)?
                }
                LogicalPlan::SubqueryAlias(_)
                | LogicalPlan::Filter(_)
                | LogicalPlan::Repartition(_)
                | LogicalPlan::Union(_)
                | LogicalPlan::Distinct(Distinct::All(_)) => {
                    self.column_rejects_null(depth + 1, index, context)?
                }
                LogicalPlan::Sort(sort) if sort.fetch.is_none() => {
                    self.column_rejects_null(depth + 1, index, context)?
                }
                LogicalPlan::Aggregate(aggregate)
                    if !aggregate
                        .group_expr
                        .iter()
                        .any(|expr| matches!(expr, Expr::GroupingSet(_))) =>
                {
                    if let Some(expr) = aggregate.group_expr.get(index) {
                        self.expression_rejects_null(depth + 1, expr, context)?
                    } else {
                        false
                    }
                }
                LogicalPlan::Join(join) => {
                    let left_width = join.left.schema().fields().len();
                    let (source, column) = match join.join_type {
                        JoinType::RightSemi | JoinType::RightAnti | JoinType::RightMark => {
                            (1, index)
                        }
                        _ if index < left_width => (0, index),
                        _ => (1, index - left_width),
                    };
                    let width = if input == 0 {
                        left_width
                    } else {
                        join.right.schema().fields().len()
                    };
                    source == input
                        && column < width
                        && self.column_rejects_null(depth + 1, column, context)?
                }
                LogicalPlan::Extension(extension) => {
                    if let Some(set) = extension
                        .node
                        .as_any()
                        .downcast_ref::<PendingSetOperation>()
                    {
                        !set.is_all
                            && input == 0
                            && self.column_rejects_null(depth + 1, index, context)?
                    } else if let Some(sort) = extension
                        .node
                        .as_any()
                        .downcast_ref::<SortWithinPartitionsNode>()
                    {
                        sort.fetch().is_none()
                            && self.column_rejects_null(depth + 1, index, context)?
                    } else {
                        false
                    }
                }
                // LIMIT, window results and materialized rows can change which rows survive.
                _ => false,
            }
        } else {
            let left_width = self.join.left.schema().fields().len();
            if self.right_side {
                index >= left_width
            } else {
                index < left_width
            }
        };
        self.memo.insert(key, rejects);
        Ok(rejects)
    }
}
