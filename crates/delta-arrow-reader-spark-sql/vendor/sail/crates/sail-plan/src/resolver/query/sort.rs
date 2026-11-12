// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::sync::Arc;

use async_recursion::async_recursion;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, DFSchema, DFSchemaRef};
use datafusion_expr::expr::{Alias, Sort};
use datafusion_expr::utils::{exprlist_to_fields, find_aggregate_exprs};
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
use crate::resolver::state::{AggregateState, PlanResolverState};

impl PlanResolver<'_> {
    pub(super) async fn resolve_query_sort(
        &self,
        input: spec::QueryPlan,
        order: Vec<spec::SortOrder>,
        is_global: bool,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let mut input = self
            .resolve_query_plan_with_hidden_fields(input, state)
            .await?;
        let sorts = self
            .resolve_query_sort_orders_by_plan(&input, &order, state)
            .await?;
        let sorts = Self::rebase_query_sort_orders(sorts, &mut input)?;
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

    /// Add aggregates used only by ORDER BY, then rebase the sort to the available outputs.
    fn rebase_query_sort_orders(sorts: Vec<Sort>, plan: &mut LogicalPlan) -> PlanResult<Vec<Sort>> {
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
            let missing = find_aggregate_exprs(sorts.iter().map(|sort| &sort.expr))
                .into_iter()
                .filter(|expr| !aggregate.aggr_expr.contains(expr))
                .collect::<Vec<_>>();
            let mut aggregate = aggregate.clone();
            if !missing.is_empty() {
                aggregate = Self::append_sort_aggregates(aggregate, missing)?;
                *input = Arc::new(Self::replace_sort_aggregate(input, &aggregate)?);
            }
            let base = Self::distinct_grouping_expressions_from_exprs(&aggregate.group_expr)
                .into_iter()
                .chain(aggregate.aggr_expr.iter().cloned())
                .collect::<Vec<_>>();
            let plan = LogicalPlan::Aggregate(aggregate);
            sorts
                .into_iter()
                .map(|x| Self::rebase_sort(x, &base, &plan))
                .collect::<PlanResult<Vec<_>>>()
        } else {
            Ok(sorts)
        }
    }

    fn append_sort_aggregates(
        mut aggregate: Aggregate,
        missing: Vec<Expr>,
    ) -> PlanResult<Aggregate> {
        let added_fields = exprlist_to_fields(&missing, &aggregate.input)?;
        // Keep the existing grouping key declarations and private grouping ID metadata.
        let fields = aggregate
            .schema
            .iter()
            .map(|(qualifier, field)| (qualifier.cloned(), Arc::clone(field)))
            .chain(added_fields)
            .collect();
        let schema = Arc::new(DFSchema::new_with_metadata(
            fields,
            aggregate.schema.metadata().clone(),
        )?);
        aggregate.aggr_expr.extend(missing);
        Ok(Aggregate::try_new_with_schema(
            aggregate.input,
            aggregate.group_expr,
            aggregate.aggr_expr,
            schema,
        )?)
    }

    fn replace_sort_aggregate(
        plan: &LogicalPlan,
        aggregate: &Aggregate,
    ) -> PlanResult<LogicalPlan> {
        let input = match plan {
            LogicalPlan::Aggregate(_) => return Ok(LogicalPlan::Aggregate(aggregate.clone())),
            LogicalPlan::Window(window) => &window.input,
            LogicalPlan::Filter(filter) => &filter.input,
            _ => {
                return Err(PlanError::internal(
                    "expected a sort input with an aggregate",
                ));
            }
        };
        let input = Self::replace_sort_aggregate(input, aggregate)?;
        Ok(plan.with_new_exprs(plan.expressions(), vec![input])?)
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
                // Aggregate arguments still use the input schema.
                if matches!(e, Expr::AggregateFunction(_)) {
                    return Ok(Transformed::new(e, false, TreeNodeRecursion::Jump));
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
                .resolve_query_sort_order_by_plan(plan, sort, true, state)
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
        allow_mixed_aggregate_inputs: bool,
        state: &mut PlanResolverState,
    ) -> PlanResult<Sort> {
        // The ordering type is checked once the key is resolved, so that a calendar interval
        // fails with Spark's error instead of retrying the child plans.
        let sort_expr = self
            .resolve_sort_order_allowing_calendar_intervals(
                sort.clone(),
                true,
                plan.schema(),
                state,
            )
            .await;
        match sort_expr {
            Ok(sort_expr) => Ok(sort_expr),
            Err(mut resolution_error) => {
                if allow_mixed_aggregate_inputs
                    && !matches!(sort.child.as_ref(), spec::Expr::Literal(_))
                    && let LogicalPlan::Projection(projection) = plan
                    && let Some(aggregate) = Self::input_aggregate(&projection.input)
                {
                    let mut merged = projection.schema.as_ref().clone();
                    merged.merge(aggregate.input.schema());
                    let merged = Arc::new(merged);
                    let mut scope = state.enter_aggregate_scope(AggregateState::Sort {
                        output: Arc::clone(&projection.schema),
                        merged: Arc::clone(&merged),
                    });
                    match self
                        .resolve_sort_order_allowing_calendar_intervals(
                            sort.clone(),
                            false,
                            &merged,
                            scope.state(),
                        )
                        .await
                    {
                        Ok(resolved) if !find_aggregate_exprs([&resolved.expr]).is_empty() => {
                            return Ok(resolved);
                        }
                        Err(error) => {
                            resolution_error = self.choose_sort_error(
                                resolution_error,
                                error,
                                &merged,
                                scope.state(),
                            );
                        }
                        _ => {}
                    }
                }
                let mut sorts = Vec::with_capacity(plan.inputs().len());
                for input_plan in plan.inputs() {
                    match self
                        .resolve_query_sort_order_by_plan(input_plan, sort, false, state)
                        .await
                    {
                        Ok(sort_expr) => sorts.push(sort_expr),
                        Err(error) => {
                            return Err(self.choose_sort_error(
                                resolution_error,
                                error,
                                input_plan.schema(),
                                state,
                            ));
                        }
                    }
                }
                if sorts.len() != 1 {
                    Err(resolution_error)
                } else {
                    Ok(sorts.one()?)
                }
            }
        }
    }

    fn choose_sort_error(
        &self,
        original: PlanError,
        fallback: PlanError,
        fallback_schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanError {
        let PlanError::UnknownColumn { name, .. } = &original else {
            return original;
        };
        // Keep retries that got past the earlier missing reference.
        if !matches!(fallback, PlanError::UnknownColumn { .. })
            || self
                .resolve_expression_attribute(name.clone(), false, fallback_schema, state)
                .is_ok()
        {
            fallback
        } else {
            original
        }
    }
}
