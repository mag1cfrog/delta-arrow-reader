use std::sync::Arc;

use datafusion_common::Result;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_expr::{Expr, Filter, LogicalPlan, lit};

use super::{contains_subquery, inline_projection_or_alias, is_projection_leaf};

pub(super) fn inline_projected_outer_exists(plan: LogicalPlan) -> Result<LogicalPlan> {
    plan.transform_up_with_subqueries(|node| {
        node.map_expressions(|expr| {
            expr.transform_up(|expr| {
                let Expr::Exists(mut exists) = expr else {
                    return Ok(Transformed::no(expr));
                };
                if exists.subquery.outer_ref_columns.is_empty()
                    || !exists.subquery.subquery.exists(|node| {
                        Ok(matches!(node, LogicalPlan::Projection(project)
                            if project.expr.iter().any(Expr::contains_outer)))
                    })?
                {
                    return Ok(Transformed::no(Expr::Exists(exists)));
                }
                let mut input = exists.subquery.subquery.as_ref();
                let mut predicate = lit(true);
                loop {
                    if contains_subquery(&predicate)? {
                        return Ok(Transformed::no(Expr::Exists(exists)));
                    }
                    if let LogicalPlan::Projection(project) = input {
                        let referenced = predicate.column_refs();
                        for (index, expr) in project.expr.iter().enumerate() {
                            let observed = referenced.iter().any(|column| {
                                project.schema.index_of_column(column).ok() == Some(index)
                            });
                            // Preparation owns unused-expression checks; do not extend its policy.
                            if !observed && !is_projection_leaf(expr) {
                                return Ok(Transformed::no(Expr::Exists(exists)));
                            }
                        }
                    }
                    if let Some((rebound, remaining)) =
                        inline_projection_or_alias(predicate.clone(), input)?
                    {
                        predicate = rebound;
                        input = remaining;
                    } else if let LogicalPlan::Filter(filter) = input {
                        predicate = predicate.and(filter.predicate.clone());
                        input = &filter.input;
                    } else {
                        break;
                    }
                }
                if !predicate.contains_outer() || !input.all_out_ref_exprs().is_empty() {
                    return Ok(Transformed::no(Expr::Exists(exists)));
                }
                // EXISTS ignores selected columns; keep its live filter on the real rows
                // so native decorrelation can extract join keys instead of a cross product.
                let input =
                    LogicalPlan::Filter(Filter::try_new(predicate, Arc::new(input.clone()))?);
                exists.subquery.outer_ref_columns = input.all_out_ref_exprs();
                exists.subquery.subquery = Arc::new(input);
                Ok(Transformed::yes(Expr::Exists(exists)))
            })
        })
    })
    .data()
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::arrow::array::{Int32Array, RecordBatch};
    use datafusion::prelude::SessionContext;
    use datafusion_common::{DataFusionError, tree_node::TreeNodeRecursion};

    use crate::config::PlanConfig;
    use crate::resolver::PlanResolver;

    async fn resolve(sql: &str) -> Result<LogicalPlan> {
        let session = SessionContext::new();
        for name in ["left_inputs", "right_inputs"] {
            session.register_batch(
                name,
                RecordBatch::try_from_iter([("k", Arc::new(Int32Array::from(vec![1, 2])) as _)])?,
            )?;
        }
        let statement = sail_sql_analyzer::parser::parse_one_statement(sql)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        let spec = sail_sql_analyzer::statement::from_ast_statement(statement)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        PlanResolver::new(&session, Arc::new(PlanConfig::default()))
            .resolve_named_plan(spec)
            .await
            .map(|named| named.plan)
            .map_err(|error| DataFusionError::External(Box::new(error)))
    }

    fn expression_nodes(plan: &LogicalPlan) -> Result<usize> {
        let mut count = 0;
        plan.apply_with_subqueries(|node| {
            node.apply_expressions(|expr| {
                expr.apply(|_| {
                    count += 1;
                    Ok(TreeNodeRecursion::Continue)
                })
            })?;
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(count)
    }

    #[tokio::test]
    async fn shared_computed_bindings_do_not_expand_exponentially() -> Result<()> {
        let mut counts = Vec::new();
        // Fail at shallow depth if a shared v + v producer starts duplicating again.
        for depth in [4, 8] {
            let mut input = "SELECT l.k AS v,r.k AS inner_k FROM right_inputs r".to_string();
            for _ in 0..depth {
                input = format!("SELECT s.v+s.v AS v,s.inner_k FROM ({input}) s");
            }
            let sql = format!(
                "SELECT l.k FROM left_inputs l WHERE EXISTS(SELECT 1 FROM ({input}) q WHERE q.v=q.inner_k)"
            );
            let before = resolve(&sql).await?;
            let after = inline_projected_outer_exists(before.clone())?;
            let input_nodes = expression_nodes(&before)?;
            let output_nodes = expression_nodes(&after)?;
            assert!(output_nodes <= input_nodes, "depth={depth}");
            counts.push(output_nodes);
        }
        assert!(counts[1] <= 2 * counts[0]);
        Ok(())
    }

    #[tokio::test]
    async fn introduced_subqueries_keep_their_captured_projection_scope() -> Result<()> {
        for value in [
            "EXISTS(SELECT 1 FROM right_inputs t WHERE t.k=s.inner_k)",
            "s.inner_k IN(SELECT t.k FROM right_inputs t)",
            "(SELECT MAX(t.k) FROM right_inputs t WHERE t.k=s.inner_k)=s.inner_k",
        ] {
            let sql = format!(
                "SELECT l.k FROM left_inputs l WHERE EXISTS(SELECT 1 FROM (
                 SELECT s.outer_k,s.inner_k,{value} AS e FROM (
                 SELECT l.k AS outer_k,r.k AS inner_k FROM right_inputs r) s
                 ) q WHERE q.outer_k=q.inner_k AND q.e)"
            );
            let before = resolve(&sql).await?;
            assert_eq!(inline_projected_outer_exists(before.clone())?, before);
        }
        Ok(())
    }

    #[tokio::test]
    async fn unobserved_computations_keep_existing_preparation_policy() -> Result<()> {
        let before = resolve(
            "SELECT l.k FROM left_inputs l WHERE EXISTS(SELECT 1 FROM (
             SELECT l.k AS outer_k,r.k AS inner_k,raise_error(CAST(r.k AS STRING)) AS unused
             FROM right_inputs r) q WHERE q.outer_k=q.inner_k)",
        )
        .await?;
        assert_eq!(inline_projected_outer_exists(before.clone())?, before);
        Ok(())
    }
}
