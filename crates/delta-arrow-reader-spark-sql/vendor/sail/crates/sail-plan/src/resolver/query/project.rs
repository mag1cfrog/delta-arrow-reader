// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::sync::Arc;

use datafusion_common::tree_node::{TreeNode, TreeNodeRewriter};
use datafusion_common::{Column, DFSchemaRef, TableReference};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::expr_rewriter::normalize_col;
use datafusion_expr::utils::{columnize_expr, expand_qualified_wildcard, expand_wildcard};
use datafusion_expr::{Expr, LogicalPlan, Projection};
use sail_common::spec;
use sail_function::scalar::multi_expr::MultiExpr;

use crate::error::{PlanError, PlanResult};
use crate::function::common::clear_inherited_field_metadata;
use crate::resolver::PlanResolver;
use crate::resolver::expression::NamedExpr;
use crate::resolver::state::PlanResolverState;
use crate::resolver::tree::PlanRewriter;
use crate::resolver::tree::explode::ExplodeRewriter;
use crate::resolver::tree::monotonic_id::MonotonicIdRewriter;
use crate::resolver::tree::spark_partition_id::SparkPartitionIdRewriter;
use crate::resolver::tree::window::WindowRewriter;

impl PlanResolver<'_> {
    pub(super) async fn resolve_query_project(
        &self,
        input: Option<spec::QueryPlan>,
        expr: Vec<spec::Expr>,
        state: &mut PlanResolverState,
    ) -> PlanResult<LogicalPlan> {
        let input = match input {
            Some(x) => self.resolve_query_plan_with_hidden_fields(x, state).await?,
            None => self.resolve_query_empty(true)?,
        };
        let schema = input.schema();
        let expr = self
            .resolve_projection_expressions(expr, schema, state)
            .await?;
        let (input, expr) = self.rewrite_wildcard(input, expr, state)?;
        let (input, expr) = self.rewrite_projection::<MonotonicIdRewriter>(input, expr, state)?;
        let (input, expr) =
            self.rewrite_projection::<SparkPartitionIdRewriter>(input, expr, state)?;
        let (input, expr) = self.rewrite_projection::<ExplodeRewriter>(input, expr, state)?;
        let (input, expr) = self.rewrite_projection::<WindowRewriter>(input, expr, state)?;
        let expr = self.rewrite_multi_expr(expr)?;
        let has_aggregate = expr.iter().any(|e| {
            e.expr
                .exists(|e| match e {
                    Expr::AggregateFunction(_) => Ok(true),
                    _ => Ok(false),
                })
                .unwrap_or(false)
        });
        if has_aggregate {
            self.rewrite_aggregate(input, expr, vec![], None, false, state)
        } else {
            let expr = self.rewrite_named_expressions(expr, state)?;
            Ok(LogicalPlan::Projection(Projection::try_new(
                expr,
                Arc::new(input),
            )?))
        }
    }

    pub(in crate::resolver) fn rewrite_wildcard(
        &self,
        input: LogicalPlan,
        expr: Vec<NamedExpr>,
        state: &mut PlanResolverState,
    ) -> PlanResult<(LogicalPlan, Vec<NamedExpr>)> {
        fn to_named_expr(
            expr: Expr,
            state: &PlanResolverState,
            qualified: bool,
        ) -> PlanResult<Option<NamedExpr>> {
            let Expr::Column(column) = expr else {
                return Err(PlanError::invalid(
                    "column expected for expanded wildcard expression",
                ));
            };
            let info = state.get_field_info(column.name())?;
            if info.is_hidden() && !qualified {
                return Ok(None);
            }
            Ok(Some(NamedExpr::new(
                vec![info.name().to_string()],
                Expr::Column(column),
            )))
        }

        fn expand_outer_wildcard(
            qualifier_filter: Option<&TableReference>,
            input_schema: &DFSchemaRef,
            state: &PlanResolverState,
            projected: &mut Vec<NamedExpr>,
        ) -> PlanResult<()> {
            if qualifier_filter.is_none() && !input_schema.fields().is_empty() {
                return Ok(());
            }

            if let Some(expected) = qualifier_filter {
                let qualifier_in_input = input_schema
                    .iter()
                    .any(|(q, _)| q.is_some_and(|q| *q == *expected));
                if qualifier_in_input {
                    return Ok(());
                }
            }

            if let Some(outer_schema) = state.get_outer_query_schema() {
                for (qualifier, field) in outer_schema.iter() {
                    if let Some(expected) = qualifier_filter
                        && qualifier.is_none_or(|q| expected != q)
                    {
                        continue;
                    }
                    let info = state.get_field_info(field.name())?;
                    if info.is_hidden() && qualifier_filter.is_none() {
                        continue;
                    }
                    let mut outer_field = field.as_ref().clone();
                    clear_inherited_field_metadata(&mut outer_field);
                    let outer_col = Expr::OuterReferenceColumn(
                        Arc::new(outer_field),
                        Column::new(qualifier.cloned(), field.name()),
                    );
                    projected.push(NamedExpr::new(vec![info.name().to_string()], outer_col));
                }
            }
            Ok(())
        }

        let schema = input.schema();
        let mut projected = vec![];
        for e in expr {
            // FIXME: wildcard options do not take into account opaque field IDs
            match e.expr {
                #[expect(deprecated)]
                Expr::Wildcard {
                    qualifier: None,
                    options,
                } => {
                    for e in expand_wildcard(schema, &input, Some(&options))? {
                        projected.extend(to_named_expr(e, state, false)?)
                    }
                    expand_outer_wildcard(None, schema, state, &mut projected)?;
                }
                #[expect(deprecated)]
                Expr::Wildcard {
                    qualifier: Some(qualifier),
                    options,
                } => {
                    let start = projected.len();
                    let qualifier_in_input = schema
                        .iter()
                        .any(|(q, _)| q.is_some_and(|q| *q == qualifier));
                    if qualifier_in_input {
                        for e in expand_qualified_wildcard(&qualifier, schema, Some(&options))? {
                            projected.extend(to_named_expr(e, state, true)?)
                        }
                    }
                    expand_outer_wildcard(Some(&qualifier), schema, state, &mut projected)?;
                    // Match Spark's qualified star order without exposing hidden
                    // keys in an unqualified star.
                    projected[start..].sort_by_key(|named| match &named.expr {
                        Expr::Column(column) | Expr::OuterReferenceColumn(_, column) => !state
                            .get_field_info(column.name())
                            .is_ok_and(|info| info.is_hidden()),
                        _ => true,
                    });
                }
                expr => projected.push(NamedExpr {
                    expr: columnize_expr(normalize_col(expr, &input)?, &input)?,
                    ..e
                }),
            }
        }
        Ok((input, projected))
    }

    pub(super) fn rewrite_projection<'s, T>(
        &self,
        input: LogicalPlan,
        expr: Vec<NamedExpr>,
        state: &'s mut PlanResolverState,
    ) -> PlanResult<(LogicalPlan, Vec<NamedExpr>)>
    where
        T: PlanRewriter<'s> + TreeNodeRewriter<Node = Expr>,
    {
        let mut rewriter = T::new_from_plan(input, state);
        let expr = expr
            .into_iter()
            .map(|e| {
                Ok(NamedExpr {
                    expr: e.expr.rewrite(&mut rewriter)?.data,
                    ..e
                })
            })
            .collect::<PlanResult<Vec<_>>>()?;
        Ok((rewriter.into_plan(), expr))
    }

    pub(in crate::resolver) fn rewrite_multi_expr(
        &self,
        expr: Vec<NamedExpr>,
    ) -> PlanResult<Vec<NamedExpr>> {
        let mut out = vec![];
        for e in expr {
            let NamedExpr {
                name,
                expr,
                metadata,
                expression_name,
                interval_qualifier,
            } = e;
            match expr {
                Expr::ScalarFunction(ScalarFunction { func, args }) => {
                    if func.inner().is::<MultiExpr>() {
                        // The metadata from the original expression are ignored.
                        if name.len() == args.len() {
                            for (name, arg) in name.into_iter().zip(args) {
                                out.push(NamedExpr::new(vec![name], arg));
                            }
                        } else {
                            for arg in args {
                                out.push(NamedExpr::try_from_alias_expr(arg)?);
                            }
                        }
                    } else {
                        out.push(NamedExpr {
                            name,
                            expr: func.call(args),
                            metadata,
                            expression_name,
                            interval_qualifier,
                        });
                    }
                }
                _ => {
                    out.push(NamedExpr {
                        name,
                        expr,
                        metadata,
                        expression_name,
                        interval_qualifier,
                    });
                }
            };
        }
        Ok(out)
    }

    /// Rewrite named expressions to DataFusion expressions.
    /// A field is registered for each name.
    /// Column expressions must refer to registered input fields.
    pub(super) fn rewrite_named_expressions(
        &self,
        expr: Vec<NamedExpr>,
        state: &mut PlanResolverState,
    ) -> PlanResult<Vec<Expr>> {
        expr.into_iter()
            .map(|e| {
                let name = if e.name.len() == 1 {
                    e.name[0].clone()
                } else {
                    let names = format!("({})", e.name.join(", "));
                    return Err(PlanError::invalid(format!(
                        "one name expected for expression, got: {names}"
                    )));
                };
                if let Expr::Column(Column { name: field_id, .. }) = &e.expr {
                    state.get_field_info(field_id)?;
                }
                // A column passed through, as by `*`, keeps its field's qualifier.
                let qualifier = e.interval_qualifier.or_else(|| match &e.expr {
                    Expr::Column(column) => state.interval_qualifier(&column.name),
                    _ => None,
                });
                let field_id = state.register_field_name(name);
                state.set_interval_qualifier(&field_id, qualifier);
                Ok(e.into_alias(field_id))
            })
            .collect()
    }
}
