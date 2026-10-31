use std::collections::HashMap;
use std::sync::Arc;

use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Column, DFSchemaRef, Spans, internal_err};
use datafusion_expr::expr::Alias;
use datafusion_expr::{
    Expr, LogicalPlan, LogicalPlanBuilder, Projection, Subquery, SubqueryAlias, build_join_schema,
};
use sail_common::spec;

use crate::error::PlanResult;
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    pub(super) async fn resolve_query_lateral_join(
        &self,
        left: spec::QueryPlan,
        right: spec::QueryPlan,
        join_condition: Option<spec::Expr>,
        join_type: spec::JoinType,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let left = self.resolve_query_plan(left, state).await?;

        // Resolve the right side in a new query scope so that column references
        // to the left side are treated as outer references (OuterReferenceColumn).
        let right = {
            let mut scope = state.enter_query_scope(left.schema().clone());
            self.resolve_query_plan(right, scope.state()).await?
        };

        let outer_ref_columns = right.all_out_ref_exprs();
        let wrapped_as_subquery = !outer_ref_columns.is_empty();

        // If the right plan contains outer references deeper than the top-level
        // Projection (e.g. in a Filter or Aggregate), wrap it as a Subquery node
        // so DataFusion's DecorrelateLateralJoin optimizer can process it.
        //
        // Before wrapping, collapse redundant Projection layers introduced by the
        // opaque field ID rename mechanism.
        let right = if wrapped_as_subquery {
            let simplified = collapse_projections(right)?;
            // Match DataFusion's SQL planner: the lateral alias stays outside
            // Subquery so decorrelation can rebuild its schema and join filters.
            match simplified {
                LogicalPlan::SubqueryAlias(SubqueryAlias { input, alias, .. }) => {
                    LogicalPlanBuilder::from(LogicalPlan::Subquery(Subquery {
                        subquery: input,
                        outer_ref_columns,
                        spans: Spans::new(),
                    }))
                    .alias(alias)?
                    .build()?
                }
                plan => LogicalPlan::Subquery(Subquery {
                    subquery: Arc::new(plan),
                    outer_ref_columns,
                    spans: Spans::new(),
                }),
            }
        } else {
            right
        };

        let df_join_type = match join_type {
            spec::JoinType::Inner | spec::JoinType::Cross => datafusion_common::JoinType::Inner,
            spec::JoinType::LeftOuter => datafusion_common::JoinType::Left,
            spec::JoinType::RightOuter => datafusion_common::JoinType::Right,
            spec::JoinType::FullOuter => datafusion_common::JoinType::Full,
            spec::JoinType::LeftSemi => datafusion_common::JoinType::LeftSemi,
            spec::JoinType::LeftAnti => datafusion_common::JoinType::LeftAnti,
            spec::JoinType::RightSemi => datafusion_common::JoinType::RightSemi,
            spec::JoinType::RightAnti => datafusion_common::JoinType::RightAnti,
        };

        let join_schema = Arc::new(build_join_schema(
            left.schema(),
            right.schema(),
            &df_join_type,
        )?);

        let condition = if let Some(cond) = join_condition {
            Some(
                self.resolve_expression(cond, &join_schema, state)
                    .await?
                    .unalias_nested()
                    .data,
            )
        } else {
            None
        };

        // For inner joins without a condition, use cross join. Otherwise use join_on.
        let plan =
            if condition.is_none() && matches!(df_join_type, datafusion_common::JoinType::Inner) {
                LogicalPlanBuilder::from(left).cross_join(right)?.build()?
            } else {
                LogicalPlanBuilder::from(left)
                    .join_on(right, df_join_type, condition)?
                    .build()?
            };

        // Keep the original output columns when decorrelation adds join keys
        // and compensation fields, including when an alias wraps the Subquery.
        let plan = if wrapped_as_subquery {
            let original_columns: Vec<Expr> = plan
                .schema()
                .columns()
                .into_iter()
                .map(Expr::Column)
                .collect();
            LogicalPlanBuilder::from(plan)
                .project(original_columns)?
                .build()?
        } else {
            plan
        };

        Ok(plan)
    }
}

fn is_rename_projection(proj: &Projection) -> bool {
    proj.expr.iter().all(|e| {
        matches!(e, Expr::Alias(Alias { expr, .. }) if matches!(expr.as_ref(), Expr::Column(_)))
    })
}

/// Collapse redundant Projection layers and duplicate SubqueryAlias nodes
/// in a plan tree. Traverses bottom-up so inner layers merge first.
///
/// Rule 1: Adjacent Projections where both are pure rename (Col AS name)
///         are merged into one. If the result is identity, it is removed.
/// Rule 2: Duplicate SubqueryAlias(t) over SubqueryAlias(t) is collapsed
///         into one.
fn collapse_projections(plan: LogicalPlan) -> datafusion_common::Result<LogicalPlan> {
    plan.transform_up(|node| {
        match node {
            // Rule 1: Merge adjacent rename Projections
            LogicalPlan::Projection(outer) if is_rename_projection(&outer) => {
                match outer.input.as_ref() {
                    LogicalPlan::Projection(inner) if is_rename_projection(inner) => {
                        let LogicalPlan::Projection(inner) =
                            Arc::unwrap_or_clone(outer.input)
                        else {
                            return internal_err!("expected Projection input");
                        };

                        let inner_map = build_output_name_map(&inner.expr);
                        let merged = inline_projection(&outer.expr, &inner_map);

                        if is_identity_projection(&merged, inner.input.schema()) {
                            Ok(Transformed::yes(Arc::unwrap_or_clone(inner.input)))
                        } else {
                            Ok(Transformed::yes(LogicalPlan::Projection(
                                Projection::try_new(merged, inner.input)?,
                            )))
                        }
                    }
                    // Projection over SubqueryAlias over Projection: skip the SubqueryAlias
                    // to check if we can merge through it.
                    // (SubqueryAlias only adds a qualifier, doesn't change field names)
                    LogicalPlan::SubqueryAlias(sa)
                        if matches!(sa.input.as_ref(), LogicalPlan::Projection(p) if is_rename_projection(p)) =>
                    {
                        let LogicalPlan::SubqueryAlias(sa) =
                            Arc::unwrap_or_clone(outer.input)
                        else {
                            return internal_err!("expected SubqueryAlias input");
                        };
                        let LogicalPlan::Projection(inner) =
                            Arc::unwrap_or_clone(sa.input)
                        else {
                            return internal_err!("expected Projection inside SubqueryAlias");
                        };

                        let inner_map = build_output_name_map(&inner.expr);
                        let merged = inline_projection(&outer.expr, &inner_map);

                        // Rebuild: merged Projection over SubqueryAlias(inner.input)
                        let new_sa = LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
                            inner.input,
                            sa.alias,
                        )?);

                        if is_identity_projection(&merged, new_sa.schema()) {
                            Ok(Transformed::yes(new_sa))
                        } else {
                            Ok(Transformed::yes(LogicalPlan::Projection(
                                Projection::try_new(merged, Arc::new(new_sa))?,
                            )))
                        }
                    }
                    _ => Ok(Transformed::no(LogicalPlan::Projection(outer))),
                }
            }

            // Rule 2: Remove duplicate SubqueryAlias
            LogicalPlan::SubqueryAlias(ref outer)
                if matches!(outer.input.as_ref(), LogicalPlan::SubqueryAlias(inner)
                    if inner.alias == outer.alias) =>
            {
                Ok(Transformed::yes(Arc::unwrap_or_clone(
                    match node {
                        LogicalPlan::SubqueryAlias(sa) => sa.input,
                        _ => return internal_err!("expected SubqueryAlias"),
                    },
                )))
            }

            other => Ok(Transformed::no(other)),
        }
    })
    .map(|t| t.data)
}

/// Build a map from output name to source expression for a Projection's exprs.
fn build_output_name_map(exprs: &[Expr]) -> HashMap<String, Expr> {
    exprs
        .iter()
        .filter_map(|e| match e {
            Expr::Alias(Alias { expr, name, .. }) => Some((name.clone(), *expr.clone())),
            Expr::Column(col) => Some((col.name.clone(), e.clone())),
            _ => None,
        })
        .collect()
}

/// For each expr in outer, replace Column references using the inner map,
/// keeping outer's alias.
fn inline_projection(outer_exprs: &[Expr], inner_map: &HashMap<String, Expr>) -> Vec<Expr> {
    outer_exprs
        .iter()
        .map(|expr| match expr {
            Expr::Alias(Alias {
                expr: inner_expr,
                relation,
                name,
                metadata,
            }) => {
                let resolved = match inner_expr.as_ref() {
                    Expr::Column(col) => inner_map
                        .get(&col.name)
                        .cloned()
                        .unwrap_or(*inner_expr.clone()),
                    other => other.clone(),
                };
                Expr::Alias(Alias {
                    expr: Box::new(resolved),
                    relation: relation.clone(),
                    name: name.clone(),
                    metadata: metadata.clone(),
                })
            }
            Expr::Column(col) => inner_map.get(&col.name).cloned().unwrap_or(expr.clone()),
            other => other.clone(),
        })
        .collect()
}

/// Check if every expr is Col(x) AS x (identity) matching the input schema.
fn is_identity_projection(exprs: &[Expr], input_schema: &DFSchemaRef) -> bool {
    let input_columns: Vec<Column> = input_schema.columns();

    if exprs.len() != input_columns.len() {
        return false;
    }

    exprs
        .iter()
        .zip(input_columns.iter())
        .all(|(expr, input_col)| match expr {
            Expr::Alias(Alias {
                expr: inner,
                name,
                relation,
                ..
            }) => {
                if let Expr::Column(col) = inner.as_ref() {
                    col == input_col && name == &col.name && *relation == col.relation
                } else {
                    false
                }
            }
            Expr::Column(col) => col == input_col,
            _ => false,
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::SessionContext;

    use crate::config::PlanConfig;
    use crate::physical_plan::SparkQueryPlanner;

    #[tokio::test]
    async fn lateral_alias_preserves_empty_group_count() -> PlanResult<()> {
        let ctx = SessionContext::new_with_state(
            SessionStateBuilder::new()
                .with_default_features()
                .with_query_planner(Arc::new(SparkQueryPlanner))
                .build(),
        );
        for (join, expected) in [
            ("CROSS JOIN", vec![Some(2), Some(1), Some(0)]),
            ("LEFT JOIN", vec![Some(2), Some(1), None]),
        ] {
            let condition = if join == "LEFT JOIN" {
                " ON t.n > 0"
            } else {
                ""
            };
            let sql = format!(
                "SELECT CAST(o.k AS BIGINT), t.n FROM VALUES (1),(2),(3) AS o(k) \
                 {join} LATERAL (SELECT count(*) AS n FROM VALUES (1),(1),(2) AS i(k) \
                 WHERE i.k=o.k) AS t{condition} ORDER BY o.k"
            );
            let ast = sail_sql_analyzer::parser::parse_one_statement(&sql)?;
            let spec = sail_sql_analyzer::statement::from_ast_statement(ast)?;
            let named = PlanResolver::new(&ctx, Arc::new(PlanConfig::default()))
                .resolve_named_plan(spec)
                .await?;
            let batches = ctx
                .execute_logical_plan(named.plan)
                .await?
                .collect()
                .await?;
            let mut rows = Vec::new();
            for batch in batches {
                assert_eq!(batch.num_columns(), 2);
                let keys = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let counts = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                rows.extend(keys.iter().zip(counts.iter()));
            }
            assert_eq!(
                rows,
                (1..=3).map(Some).zip(expected).collect::<Vec<_>>(),
                "{sql}"
            );
        }
        Ok(())
    }
}
