//! UNION DISTINCT normally exposes normalized floating zeros. Spark can retain
//! a constant's original sign when it removes identical branches before folding.
//! UNION ALL retains the values and multiplicities from every branch.

use std::sync::Arc;

use datafusion::datasource::source_as_provider;
use datafusion::execution::SessionState;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{DFSchema, Result};
use datafusion_expr::{Expr, LogicalPlan, LogicalPlanBuilder, Union, Volatility};
use sail_common::spec::SetOpType;

use super::constant_columns::build_set_result_constant_markers;
use super::filter_context::EnclosingQueryContext;
use super::pending::{
    PendingSetOperation, SetLoweringResult, allow_nullable_float_results, lower_sets,
};
use super::{distinct_set_operation_rows, set_result_expressions};
use crate::resolver::state::PlanResolverState;

pub(super) fn lower_union<'a>(
    plan: &'a LogicalPlan,
    set: &'a PendingSetOperation,
    context: &mut EnclosingQueryContext<'a>,
    session: &SessionState,
    state: &mut PlanResolverState,
) -> Result<SetLoweringResult> {
    let mut branches = Vec::new();
    for (index, input) in [&set.left, &set.right].into_iter().enumerate() {
        let saved = context.enter_input(plan, index);
        if set.is_all {
            branches.push((input.as_ref(), lower_sets(input, context, session, state)?));
        } else {
            lower_distinct_union_branches(input, context, session, state, &mut branches)?;
        }
        context.leave_input(saved);
    }
    let first = &branches[0];
    let mut same_branches = !set.is_all;
    for (input, _) in &branches[1..] {
        if !same_branches {
            break;
        }
        same_branches = branches_match_before_folding(first.0, input)?;
    }
    let constants = if same_branches {
        build_set_result_constant_markers(&first.1.constant_analysis, session)?
    } else {
        vec![None; plan.schema().fields().len()]
    };
    let under_limit_one = !set.is_all && context.has_single_row_limit()?;
    let build_union = |inputs: Vec<Arc<LogicalPlan>>| -> Result<LogicalPlan> {
        let mut union = Union::try_new(inputs)?;
        // Native UNION keeps its schema when simplification removes nonnull constant markers.
        union.schema = allow_nullable_float_results(&union.schema)?;
        let union = LogicalPlan::Union(union);
        if set.is_all {
            return rename_union_columns(union, plan.schema());
        }
        let mut constants = constants.clone();
        for (marker, field) in constants.iter_mut().zip(union.schema().fields()) {
            if let Some(marker) = marker {
                marker.nullable |= field.is_nullable();
            }
        }
        let expressions = set_result_expressions(&union, &constants, under_limit_one)?;
        let projected = LogicalPlanBuilder::from(union)
            .project(expressions)?
            .build()?;
        let result = if under_limit_one {
            projected
        } else {
            distinct_set_operation_rows(projected)?
        };
        rename_union_columns(result, plan.schema())
    };
    let executable = build_union(
        branches
            .iter()
            .map(|(_, branch)| Arc::new(branch.executable.clone()))
            .collect(),
    )?;
    let constant_analysis = if same_branches {
        // Removing a duplicate branch only in the analysis copy exposes its constants.
        rename_union_columns(first.1.constant_analysis.clone(), plan.schema())?
    } else {
        build_union(
            branches
                .into_iter()
                .map(|(_, branch)| Arc::new(branch.constant_analysis))
                .collect(),
        )?
    };
    Ok(SetLoweringResult {
        executable,
        constant_analysis,
    })
}

fn lower_distinct_union_branches<'a>(
    plan: &'a LogicalPlan,
    context: &mut EnclosingQueryContext<'a>,
    session: &SessionState,
    state: &mut PlanResolverState,
    branches: &mut Vec<(&'a LogicalPlan, SetLoweringResult)>,
) -> Result<()> {
    let inputs = match plan {
        LogicalPlan::Extension(extension) => extension
            .node
            .as_any()
            .downcast_ref::<PendingSetOperation>()
            .filter(|set| set.set_op_type == SetOpType::Union)
            .map(|set| vec![set.left.as_ref(), set.right.as_ref()]),
        LogicalPlan::Union(union) => Some(union.inputs.iter().map(Arc::as_ref).collect()),
        _ => transparent_input(plan).map(|input| vec![input]),
    };
    if let Some(inputs) = inputs {
        for (index, input) in inputs.into_iter().enumerate() {
            let saved = context.enter_input(plan, index);
            lower_distinct_union_branches(input, context, session, state, branches)?;
            context.leave_input(saved);
        }
    } else {
        branches.push((plan, lower_sets(plan, context, session, state)?));
    }
    Ok(())
}

fn rename_union_columns(input: LogicalPlan, output: &DFSchema) -> Result<LogicalPlan> {
    let expressions = input
        .schema()
        .iter()
        .zip(output.iter())
        .map(|(source, (qualifier, field))| {
            Expr::from(source).alias_qualified_with_metadata(
                qualifier.cloned(),
                field.name(),
                Some(field.metadata().clone().into()),
            )
        })
        .collect::<Vec<_>>();
    LogicalPlanBuilder::from(input)
        .project(expressions)?
        .build()
}

fn transparent_input(plan: &LogicalPlan) -> Option<&LogicalPlan> {
    match plan {
        LogicalPlan::SubqueryAlias(alias) => Some(&alias.input),
        LogicalPlan::Projection(projection)
            if projection.expr.len() == projection.input.schema().fields().len()
                && projection
                    .expr
                    .iter()
                    .zip(projection.input.schema().iter())
                    .all(|(expr, field)| {
                        expr.clone().unalias_nested().data == Expr::from(field)
                    }) =>
        {
            Some(&projection.input)
        }
        _ => None,
    }
}

// shortcut: prove identity only for these resolved operators; extend with an independent Spark case.
fn branches_match_before_folding(left: &LogicalPlan, right: &LogicalPlan) -> Result<bool> {
    if let Some(input) = transparent_input(left) {
        return branches_match_before_folding(input, right);
    }
    if let Some(input) = transparent_input(right) {
        return branches_match_before_folding(left, input);
    }
    let same_operator = match (left, right) {
        (LogicalPlan::Projection(_), LogicalPlan::Projection(_)) => true,
        (LogicalPlan::Filter(_), LogicalPlan::Filter(_)) => true,
        (LogicalPlan::Aggregate(left), LogicalPlan::Aggregate(right)) => {
            left.group_expr.len() == right.group_expr.len()
                && left.aggr_expr.len() == right.aggr_expr.len()
        }
        (LogicalPlan::EmptyRelation(left), LogicalPlan::EmptyRelation(right)) => {
            return Ok(left == right);
        }
        (LogicalPlan::TableScan(left), LogicalPlan::TableScan(right)) => {
            let same_source = Arc::ptr_eq(&left.source, &right.source)
                || match (
                    source_as_provider(&left.source),
                    source_as_provider(&right.source),
                ) {
                    (Ok(left), Ok(right)) => Arc::ptr_eq(&left, &right),
                    _ => false,
                };
            return Ok(same_source && left == right);
        }
        (LogicalPlan::Extension(left), LogicalPlan::Extension(right)) => {
            match (
                left.node.as_any().downcast_ref::<PendingSetOperation>(),
                right.node.as_any().downcast_ref::<PendingSetOperation>(),
            ) {
                (Some(left), Some(right)) => {
                    left.set_op_type == right.set_op_type && left.is_all == right.is_all
                }
                _ => false,
            }
        }
        _ => false,
    };
    if !same_operator || left.schema().fields().len() != right.schema().fields().len() {
        return Ok(false);
    }
    let left_inputs = left.inputs();
    let right_inputs = right.inputs();
    if left_inputs.len() != right_inputs.len() {
        return Ok(false);
    }
    for (left, right) in left_inputs.iter().zip(&right_inputs) {
        if !branches_match_before_folding(left, right)? {
            return Ok(false);
        }
    }
    let left_expressions = left.expressions();
    let right_expressions = right.expressions();
    if left_expressions.len() != right_expressions.len() {
        return Ok(false);
    }
    for (left, right) in left_expressions.into_iter().zip(right_expressions) {
        let mut immutable = true;
        left.apply(|expr| {
            immutable &= match expr {
                Expr::ScalarFunction(function) => {
                    function.func.signature().volatility == Volatility::Immutable
                }
                Expr::HigherOrderFunction(function) => {
                    function.func.signature().volatility == Volatility::Immutable
                }
                Expr::ScalarVariable(..)
                | Expr::ScalarSubquery(..)
                | Expr::Exists(..)
                | Expr::InSubquery(..) => false,
                _ => true,
            };
            Ok(TreeNodeRecursion::Continue)
        })?;
        if !immutable {
            return Ok(false);
        }
        let left = left
            .transform_up(|expr| {
                if let Expr::Column(column) = &expr {
                    for (left, right) in left_inputs.iter().zip(&right_inputs) {
                        if let Ok(index) = left.schema().index_of_column(column) {
                            return Ok(Transformed::yes(Expr::from(
                                right.schema().qualified_field(index),
                            )));
                        }
                    }
                }
                Ok(Transformed::no(expr))
            })?
            .data;
        if left.unalias_nested().data != right.unalias_nested().data {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::{MemTable, TableProvider, provider_as_source};

    #[test]
    fn same_scan_name_requires_the_same_provider() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Float64, true)]));
        let first: Arc<dyn TableProvider> =
            Arc::new(MemTable::try_new(schema.clone(), vec![vec![]])?);
        let other: Arc<dyn TableProvider> = Arc::new(MemTable::try_new(schema, vec![vec![]])?);
        let scan =
            |provider| LogicalPlanBuilder::scan("t", provider_as_source(provider), None)?.build();
        let left = scan(first.clone())?;
        let repeated = scan(first)?;
        let different = scan(other)?;
        assert_eq!(left, different, "native scan equality omits the provider");
        assert!(branches_match_before_folding(&left, &repeated)?);
        assert!(!branches_match_before_folding(&left, &different)?);
        Ok(())
    }
}
