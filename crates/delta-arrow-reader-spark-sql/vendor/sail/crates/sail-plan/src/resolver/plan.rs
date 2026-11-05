// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use datafusion::logical_expr::LogicalPlan;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_expr::Expr;
use datafusion_expr::expr::AggregateFunction;
use datafusion_expr::utils::{find_aggregate_exprs, find_window_exprs};
use sail_common::spec;

use crate::error::{PlanError, PlanResult};
use crate::function::expression_is_nondeterministic;
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;

#[derive(Debug)]
pub struct NamedPlan {
    pub plan: LogicalPlan,
    /// The user-facing query field names.
    pub fields: Vec<String>,
}

impl PlanResolver<'_> {
    /// Resolves a query into a named plan.
    pub async fn resolve_named_plan(&self, query: spec::QueryPlan) -> PlanResult<NamedPlan> {
        let mut state = PlanResolverState::new();
        let plan = self.resolve_query_plan(query, &mut state).await?;
        Self::validate_aggregate_arguments(&plan)?;
        let plan = self.lower_resolved_set_operations(plan, &mut state)?;
        let plan = Self::preserve_order_sensitive_aggregate_sorts(plan)?;
        let fields = Self::get_field_names(plan.schema(), &state)?;
        Ok(NamedPlan { plan, fields })
    }

    pub(super) fn validate_aggregate_arguments(plan: &LogicalPlan) -> PlanResult<()> {
        let mut functions: Vec<AggregateFunction> = Vec::new();
        plan.apply_with_subqueries(|plan| {
            plan.apply_expressions(|expression| {
                expression.apply(|expression| {
                    if let Expr::AggregateFunction(function) = expression {
                        functions.push(function.clone());
                    }
                    Ok(TreeNodeRecursion::Continue)
                })
            })?;
            Ok(TreeNodeRecursion::Continue)
        })?;

        // All fields are typed; Spark checks windows before each aggregate's other errors.
        for function in &functions {
            let count_if = function
                .func
                .aliases()
                .iter()
                .any(|alias| alias == "count_if");
            if !find_window_exprs(&function.params.args).is_empty() {
                return Err(PlanError::invalid(if count_if {
                    "COUNT_IF argument cannot contain a window function"
                } else {
                    "[_LEGACY_ERROR_TEMP_1031] An aggregate argument cannot contain a window function"
                }));
            }
            if count_if
                && let Some(filter) = &function.params.filter
                && !find_window_exprs([filter.as_ref()]).is_empty()
            {
                return Err(PlanError::invalid(
                    "COUNT_IF FILTER cannot contain a window function",
                ));
            }
        }
        for function in &functions {
            let count_if = function
                .func
                .aliases()
                .iter()
                .any(|alias| alias == "count_if");
            if !find_aggregate_exprs(&function.params.args).is_empty() {
                return Err(PlanError::invalid(if count_if {
                    "COUNT_IF argument cannot contain an aggregate function"
                } else {
                    "[NESTED_AGGREGATE_FUNCTION] An aggregate argument cannot contain an aggregate function"
                }));
            }
            if count_if
                && let Some(filter) = &function.params.filter
                && !find_aggregate_exprs([filter.as_ref()]).is_empty()
            {
                return Err(PlanError::invalid(
                    "COUNT_IF FILTER cannot contain an aggregate function",
                ));
            }
            if let Some(argument) = function
                .params
                .args
                .iter()
                .find(|arg| expression_is_nondeterministic(arg))
            {
                if count_if {
                    return Err(PlanError::invalid(
                        "COUNT_IF argument requires a deterministic expression",
                    ));
                }
                return Err(PlanError::analysis(format!(
                    "[AGGREGATE_FUNCTION_WITH_NONDETERMINISTIC_EXPRESSION] Non-deterministic expression {argument} should not appear in the arguments of aggregate function {}",
                    function.func.name()
                )));
            }
            if count_if
                && function
                    .params
                    .filter
                    .as_deref()
                    .is_some_and(expression_is_nondeterministic)
            {
                return Err(PlanError::invalid(
                    "COUNT_IF FILTER requires a deterministic expression",
                ));
            }
        }
        Ok(())
    }
}
