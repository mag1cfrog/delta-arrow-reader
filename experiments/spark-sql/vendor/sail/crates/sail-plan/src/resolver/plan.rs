// Modified from Sail v0.7.1 for the Delta reader experiment. See experiments/spark-sql/UPSTREAM.md in the host repository.
use datafusion::logical_expr::LogicalPlan;
use sail_common::spec;

use crate::error::PlanResult;
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
        let plan = Self::preserve_order_sensitive_aggregate_sorts(plan)?;
        let fields = Self::get_field_names(plan.schema(), &state)?;
        Ok(NamedPlan { plan, fields })
    }
}
