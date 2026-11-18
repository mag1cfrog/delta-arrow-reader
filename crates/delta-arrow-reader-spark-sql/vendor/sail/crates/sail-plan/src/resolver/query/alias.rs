// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
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
        let input = self
            .resolve_query_plan_with_file_metadata(input, state)
            .await?;
        let schema = input.schema();
        let input = if columns.is_empty() {
            input
        } else {
            let visible = schema
                .columns()
                .into_iter()
                .filter(|column| {
                    state
                        .get_field_info(column.name())
                        .is_ok_and(|info| info.file_metadata().is_none())
                })
                .collect::<Vec<_>>();
            if columns.len() != visible.len() {
                return Err(PlanError::invalid(format!(
                    "number of column names ({}) does not match number of columns ({})",
                    columns.len(),
                    visible.len()
                )));
            }
            let mut expr: Vec<Expr> = visible
                .into_iter()
                .zip(columns)
                .map(|(col, name)| {
                    let qualifier = state.interval_qualifier(col.name());
                    let field_id = state.register_field_name(name);
                    state.set_interval_qualifier(&field_id, qualifier);
                    Expr::Column(col.clone()).alias(field_id)
                })
                .collect();
            expr.extend(
                schema
                    .columns()
                    .into_iter()
                    .filter(|column| {
                        state
                            .get_field_info(column.name())
                            .is_ok_and(|info| info.file_metadata().is_some())
                    })
                    .map(Expr::Column),
            );
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
