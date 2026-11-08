use datafusion_common::{Column, DFSchemaRef};
use datafusion_expr::{ExprSchemable, expr};
use sail_common::spec;

use crate::error::{PlanError, PlanResult};
use crate::function::common::reject_calendar_interval_sort_key;
use crate::resolver::PlanResolver;
use crate::resolver::expression::NamedExpr;
use crate::resolver::state::PlanResolverState;

pub(super) fn sort_order_options(
    direction: spec::SortDirection,
    null_ordering: spec::NullOrdering,
) -> (bool, bool) {
    use spec::{NullOrdering, SortDirection};

    let asc = !matches!(direction, SortDirection::Descending);
    let nulls_first = match null_ordering {
        NullOrdering::NullsFirst => true,
        NullOrdering::NullsLast => false,
        NullOrdering::Unspecified => asc,
    };
    (asc, nulls_first)
}

impl PlanResolver<'_> {
    pub(super) async fn resolve_expression_sort_order(
        &self,
        sort: spec::SortOrder,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let sort = self.resolve_sort_order(sort, true, schema, state).await?;
        Ok(NamedExpr::new(vec![], sort.expr))
    }

    /// A sort key, which like Spark cannot be a calendar interval.
    pub(in super::super) async fn resolve_sort_order(
        &self,
        sort: spec::SortOrder,
        resolve_literals: bool,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<expr::Sort> {
        let sort = self
            .resolve_sort_order_allowing_calendar_intervals(sort, resolve_literals, schema, state)
            .await?;
        if let Ok(data_type) = sort.expr.get_type(schema) {
            reject_calendar_interval_sort_key(&data_type)?;
        }
        Ok(sort)
    }

    /// A sort key that may be a calendar interval, for callers that try several plans and
    /// check the key once it resolves.
    pub(in super::super) async fn resolve_sort_order_allowing_calendar_intervals(
        &self,
        sort: spec::SortOrder,
        resolve_literals: bool,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<expr::Sort> {
        let spec::SortOrder {
            child,
            direction,
            null_ordering,
        } = sort;
        let (asc, nulls_first) = sort_order_options(direction, null_ordering);

        match child.as_ref() {
            spec::Expr::Literal(literal) if resolve_literals => {
                let num_fields = schema.fields().len();
                let position = match literal {
                    spec::Literal::Int32 { value: Some(value) } => *value as usize,
                    spec::Literal::Int64 { value: Some(value) } => *value as usize,
                    _ => {
                        return Ok(expr::Sort {
                            expr: self.resolve_expression(*child, schema, state).await?,
                            asc,
                            nulls_first,
                        });
                    }
                };
                if position > 0 && position <= num_fields {
                    Ok(expr::Sort {
                        expr: expr::Expr::Column(Column::from(
                            schema.qualified_field(position - 1),
                        )),
                        asc,
                        nulls_first,
                    })
                } else {
                    Err(PlanError::invalid(format!(
                        "Cannot resolve column position {position}. Valid positions are 1 to {num_fields}."
                    )))
                }
            }
            _ => Ok(expr::Sort {
                expr: self.resolve_expression(*child, schema, state).await?,
                asc,
                nulls_first,
            }),
        }
    }

    pub(in super::super) async fn resolve_sort_orders(
        &self,
        sort: Vec<spec::SortOrder>,
        resolve_literals: bool,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<Vec<expr::Sort>> {
        let mut results: Vec<expr::Sort> = Vec::with_capacity(sort.len());
        for s in sort {
            let expr = self
                .resolve_sort_order(s, resolve_literals, schema, state)
                .await?;
            results.push(expr);
        }
        Ok(results)
    }
}
