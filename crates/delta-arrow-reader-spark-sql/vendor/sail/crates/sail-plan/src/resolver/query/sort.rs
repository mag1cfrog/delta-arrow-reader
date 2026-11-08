// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::sync::Arc;

use async_recursion::async_recursion;
use datafusion_common::Column;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion_expr::expr::{Alias, Sort};
use datafusion_expr::{
    Aggregate, Expr, ExprSchemable, Extension, LogicalPlan, LogicalPlanBuilder, Projection, Window,
};
use sail_common::spec;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_logical_plan::sort::SortWithinPartitionsNode;

use crate::error::{PlanError, PlanResult};
use crate::function::common::{
    preserve_hidden_sort_output_declaration, reject_calendar_interval_sort_key,
};
use crate::function::sort_keys::spark_sort_key;
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    pub(super) async fn resolve_query_sort(
        &self,
        input: spec::QueryPlan,
        order: Vec<spec::SortOrder>,
        is_global: bool,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = self
            .resolve_query_plan_with_hidden_fields(input, state)
            .await?;
        let sorts = self
            .resolve_query_sort_orders_by_plan(&input, &order, state)
            .await?;
        let sorts = Self::rebase_query_sort_orders(sorts, &input)?;
        let mut plan = LogicalPlanBuilder::from(input).sort(sorts)?.build()?;
        // The builder adds the sort, with any columns its keys need, and an optional projection
        // above it, so each key's type is known against the sort's input.
        let sort = match &plan {
            LogicalPlan::Projection(projection) => projection.input.as_ref(),
            plan => plan,
        };
        if let LogicalPlan::Sort(sort) = sort {
            for key in &sort.expr {
                if let Ok(data_type) = key.expr.get_type(sort.input.schema()) {
                    reject_calendar_interval_sort_key(&data_type)?;
                }
            }
        }
        if let LogicalPlan::Projection(project) = plan {
            // Hidden ORDER BY columns add an output projection with its own nested declaration.
            let expressions = project
                .expr
                .into_iter()
                .zip(project.schema.iter())
                .map(|(expression, (qualifier, field))| {
                    preserve_hidden_sort_output_declaration(expression, field)
                        .alias_qualified(qualifier.cloned(), field.name())
                })
                .collect();
            plan = LogicalPlan::Projection(Projection::try_new(expressions, project.input)?);
        }
        // Grouping placeholders and hidden sort columns are resolved before key typing.
        Ok(plan
            .transform_down(|plan| {
                let LogicalPlan::Sort(mut sort) = plan else {
                    return Ok(Transformed::no(plan));
                };
                let output = sort
                    .input
                    .schema()
                    .columns()
                    .into_iter()
                    .map(Expr::Column)
                    .collect::<Vec<_>>();
                let mut projected = output.clone();
                for expression in &mut sort.expr {
                    expression.expr = spark_sort_key(expression.expr.clone(), sort.input.schema())?;
                    if expression.expr.is_volatile() {
                        // A distinct ORDER BY value is evaluated once, then reused by sort/merge.
                        let field = state.register_hidden_field_name("__sort_value");
                        projected.push(expression.expr.clone().alias(&field));
                        expression.expr = Expr::Column(Column::from_name(field));
                    }
                }
                let materialized = projected.len() != output.len();
                if materialized {
                    sort.input = Arc::new(LogicalPlan::Projection(Projection::try_new(
                        projected, sort.input,
                    )?));
                }
                let plan = if is_global {
                    LogicalPlan::Sort(sort)
                } else {
                    LogicalPlan::Extension(Extension {
                        node: Arc::new(SortWithinPartitionsNode::new(
                            sort.input, sort.expr, sort.fetch,
                        )),
                    })
                };
                let plan = if materialized {
                    LogicalPlan::Projection(Projection::try_new(output, Arc::new(plan))?)
                } else {
                    plan
                };
                // The builder adds only this sort and an optional projection above it.
                Ok(Transformed::new(plan, true, TreeNodeRecursion::Jump))
            })
            .data()?)
    }

    /// Rebase sort expressions using aggregation expressions when the aggregate plan
    /// is inside a projection plan.
    /// Usually the [LogicalPlanBuilder] handles rebase, but this particular case is not handled yet.
    /// We do not do so recursively to make sure this workaround is only applied to a particular pattern.
    ///
    /// This workaround is needed for queries where the aggregation expression is aliased.
    /// Here is an example.
    /// ```sql
    /// SELECT a, sum(b) AS s FROM VALUES (1, 2) AS t(a, b) GROUP BY a ORDER BY sum(b)
    /// ```
    fn rebase_query_sort_orders(sorts: Vec<Sort>, plan: &LogicalPlan) -> PlanResult<Vec<Sort>> {
        let LogicalPlan::Projection(Projection { input, expr, .. }) = plan else {
            return Ok(sorts);
        };
        let aggregate = Self::input_aggregate(input.as_ref());
        let sorts = sorts
            .iter()
            .map(|x| Self::rebase_sort_to_projection_input(x.clone(), expr))
            .collect::<PlanResult<Vec<_>>>()?;
        let sorts = if let Some(aggregate) = aggregate {
            sorts
                .into_iter()
                .map(|sort| Self::rewrite_sort_grouping_expr(sort, aggregate))
                .collect::<PlanResult<Vec<_>>>()?
        } else {
            sorts
        };
        let sorts = sorts
            .iter()
            .map(|x| Self::rebase_sort_to_projection_output(x.clone(), expr))
            .collect::<PlanResult<Vec<_>>>()?;

        if let Some(aggregate) = aggregate {
            let base = Self::distinct_grouping_expressions_from_exprs(&aggregate.group_expr)
                .into_iter()
                .chain(aggregate.aggr_expr.iter().cloned())
                .collect::<Vec<_>>();
            let plan = LogicalPlan::Aggregate(aggregate.clone());
            sorts
                .into_iter()
                .map(|x| Self::rebase_sort(x, &base, &plan))
                .collect::<PlanResult<Vec<_>>>()
        } else {
            Ok(sorts)
        }
    }

    fn input_aggregate(plan: &LogicalPlan) -> Option<&Aggregate> {
        match plan {
            LogicalPlan::Aggregate(aggregate) => Some(aggregate),
            // HAVING filters and windows preserve the aggregate's fields.
            LogicalPlan::Window(Window { input, .. }) => Self::input_aggregate(input),
            LogicalPlan::Filter(filter) => Self::input_aggregate(&filter.input),
            _ => None,
        }
    }

    fn rewrite_sort_grouping_expr(sort: Sort, aggregate: &Aggregate) -> PlanResult<Sort> {
        let Sort {
            expr,
            asc,
            nulls_first,
        } = sort;
        let has_grouping_set = Self::has_grouping_set(&aggregate.group_expr);
        let native_keys = Self::distinct_grouping_expressions_from_exprs(&aggregate.group_expr);
        let grouping_order = if has_grouping_set {
            Self::grouping_key_order(aggregate, native_keys.len())?
        } else {
            vec![]
        };
        let grouping_exprs = if grouping_order.is_empty() {
            native_keys
        } else {
            grouping_order
                .iter()
                .map(|&index| native_keys[index].clone())
                .collect()
        };
        let expr =
            Self::rewrite_grouping_expr(expr, &grouping_exprs, has_grouping_set, &grouping_order)?;
        Ok(Sort {
            expr,
            asc,
            nulls_first,
        })
    }

    fn rebase_sort_to_projection_input(sort: Sort, projection: &[Expr]) -> PlanResult<Sort> {
        let Sort {
            expr,
            asc,
            nulls_first,
        } = sort;
        let find = |col: &Column| -> Option<Expr> {
            projection.iter().find_map(|expr| {
                if let Expr::Alias(Alias {
                    expr,
                    relation,
                    name,
                    ..
                }) = expr
                    && relation == &col.relation
                    && name == &col.name
                {
                    return Some(expr.as_ref().clone());
                }
                None
            })
        };
        let expr = expr
            .transform_down(|e| {
                if let Expr::Column(ref col) = e
                    && let Some(expr) = find(col)
                {
                    return Ok(Transformed::yes(expr));
                }
                Ok(Transformed::no(e))
            })
            .data()?;
        Ok(Sort {
            expr,
            asc,
            nulls_first,
        })
    }

    fn rebase_sort_to_projection_output(sort: Sort, projection: &[Expr]) -> PlanResult<Sort> {
        let Sort {
            expr,
            asc,
            nulls_first,
        } = sort;
        let find = |target: &Expr| -> Option<Expr> {
            projection.iter().find_map(|expr| {
                if let Expr::Alias(Alias {
                    expr,
                    relation,
                    name,
                    ..
                }) = expr
                    && expr.as_ref() == target
                {
                    return Some(Expr::Column(Column::new(relation.clone(), name.clone())));
                }
                None
            })
        };
        let expr = expr
            .transform_down(|e| {
                if let Some(expr) = find(&e) {
                    return Ok(Transformed::yes(expr));
                }
                Ok(Transformed::no(e))
            })
            .data()?;
        Ok(Sort {
            expr,
            asc,
            nulls_first,
        })
    }

    fn rebase_sort(sort: Sort, base: &[Expr], plan: &LogicalPlan) -> PlanResult<Sort> {
        let Sort {
            expr,
            asc,
            nulls_first,
        } = sort;
        let expr = Self::rebase_expression(expr, base, plan)?;
        Ok(Sort {
            expr,
            asc,
            nulls_first,
        })
    }

    /// Resolve sort orders by attempting child plans recursively.
    async fn resolve_query_sort_orders_by_plan(
        &self,
        plan: &LogicalPlan,
        sorts: &[spec::SortOrder],
        state: &mut PlanResolverState,
    ) -> PlanResult<Vec<Sort>> {
        let mut results: Vec<Sort> = Vec::with_capacity(sorts.len());
        for sort in sorts {
            let expr = self
                .resolve_query_sort_order_by_plan(plan, sort, state)
                .await?;
            results.push(expr);
        }
        Ok(results)
    }

    /// Resolve a sort order by attempting child plans recursively.
    /// This is needed since the sort order may refer to a column in a child plan,
    /// So we need to use the schema of the child plan to map between user-facing
    /// field name and the opaque field ID.
    #[async_recursion]
    async fn resolve_query_sort_order_by_plan(
        &self,
        plan: &LogicalPlan,
        sort: &spec::SortOrder,
        state: &mut PlanResolverState,
    ) -> PlanResult<Sort> {
        // The ordering type is checked once the key is resolved, so that a calendar interval
        // fails with Spark's error instead of retrying the child plans.
        let sort_expr = self
            .resolve_sort_order_without_type_check(sort.clone(), true, plan.schema(), state)
            .await;
        match sort_expr {
            Ok(sort_expr) => Ok(sort_expr),
            Err(_) => {
                let mut sorts = Vec::with_capacity(plan.inputs().len());
                for input_plan in plan.inputs() {
                    let sort_expr = self
                        .resolve_query_sort_order_by_plan(input_plan, sort, state)
                        .await?;
                    sorts.push(sort_expr);
                }
                if sorts.len() != 1 {
                    Err(PlanError::invalid(format!("sort expression: {sort:?}")))
                } else {
                    Ok(sorts.one()?)
                }
            }
        }
    }
}
