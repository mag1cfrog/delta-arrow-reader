// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::sync::Arc;

use datafusion_common::DFSchema;
use datafusion_common::tree_node::{Transformed, TreeNodeRecursion};
use datafusion_expr::{EmptyRelation, Expr, LogicalPlan};

use crate::function::aggregate_argument_needs_validation;
use crate::resolver::state::PlanResolverState;

pub(crate) mod explode;
pub(crate) mod monotonic_id;
pub(crate) mod spark_partition_id;
pub(crate) mod window;

fn skip_aggregate_arguments_needing_validation(node: Expr) -> Transformed<Expr> {
    // Hoisting must not erase arguments before typed-query validation inspects them.
    let preserve = match &node {
        Expr::AggregateFunction(function) => {
            function
                .params
                .args
                .iter()
                .any(aggregate_argument_needs_validation)
                || (function
                    .func
                    .aliases()
                    .iter()
                    .any(|alias| alias == "count_if")
                    && function
                        .params
                        .filter
                        .as_deref()
                        .is_some_and(aggregate_argument_needs_validation))
        }
        _ => false,
    };
    let recursion = if preserve {
        TreeNodeRecursion::Jump
    } else {
        TreeNodeRecursion::Continue
    };
    Transformed::new(node, false, recursion)
}

fn empty_logical_plan() -> LogicalPlan {
    LogicalPlan::EmptyRelation(EmptyRelation {
        produce_one_row: false,
        schema: Arc::new(DFSchema::empty()),
    })
}

pub(crate) trait PlanRewriter<'s> {
    fn new_from_plan(plan: LogicalPlan, state: &'s mut PlanResolverState) -> Self;
    fn into_plan(self) -> LogicalPlan;
}
