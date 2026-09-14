// Modified from Sail v0.7.1 for the Delta reader experiment. See experiments/spark-sql/UPSTREAM.md in the host repository.
use std::sync::Arc;

use datafusion_common::TableReference;
use datafusion_expr::{Expr, LogicalPlan, Projection, SubqueryAlias};
use sail_common::spec;

use crate::error::{PlanError, PlanResult};
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    pub(super) async fn resolve_query_table_alias(
        &self,
        input: spec::QueryPlan,
        name: spec::Identifier,
        columns: Vec<spec::Identifier>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self.resolve_query_plan(input, state).await?;
        let schema = input.schema();
        let input = if columns.is_empty() {
            input
        } else {
            if columns.len() != schema.fields().len() {
                return Err(PlanError::invalid(format!(
                    "number of column names ({}) does not match number of columns ({})",
                    columns.len(),
                    schema.fields().len()
                )));
            }
            let expr: Vec<Expr> = schema
                .columns()
                .into_iter()
                .zip(columns)
                .map(|(col, name)| Expr::Column(col.clone()).alias(state.register_field_name(name)))
                .collect();
            LogicalPlan::Projection(Projection::try_new(expr, Arc::new(input))?)
        };
        Ok(LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
            Arc::new(input),
            TableReference::Bare {
                table: Arc::from(String::from(name)),
            },
        )?))
    }
}
