use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion_common::alias::AliasGenerator;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, DFSchema, Result};
use datafusion_expr::expr_rewriter::{NamePreserver, strip_outer_reference};
use datafusion_expr::utils::find_out_reference_exprs;
use datafusion_expr::{
    Expr, Filter, LogicalPlan, LogicalPlanBuilder, Projection, Subquery, SubqueryAlias,
};

use super::{contains_subquery, is_projection_leaf};

pub(super) fn materialize_outer_exists_bindings(
    plan: LogicalPlan,
    aliases: &AliasGenerator,
) -> Result<LogicalPlan> {
    plan.transform_up_with_subqueries(|node| {
        let names = NamePreserver::new(&node);
        match node {
            LogicalPlan::Projection(mut project)
                if project.input.all_out_ref_exprs().is_empty() =>
            {
                let mut changed = false;
                for expr in &mut project.expr {
                    let name = names.save(expr);
                    let rebound =
                        materialize_in_expression(expr.clone(), &mut project.input, aliases)?;
                    changed |= rebound.transformed;
                    *expr = name.restore(rebound.data);
                }
                Ok(Transformed::new_transformed(
                    LogicalPlan::Projection(project),
                    changed,
                ))
            }
            LogicalPlan::Filter(mut filter) if filter.input.all_out_ref_exprs().is_empty() => {
                let schema = Arc::clone(filter.input.schema());
                let rebound =
                    materialize_in_expression(filter.predicate, &mut filter.input, aliases)?;
                filter.predicate = rebound.data;
                let plan = LogicalPlan::Filter(filter);
                Ok(if rebound.transformed {
                    Transformed::yes(LogicalPlan::Projection(Projection::new_from_schema(
                        Arc::new(plan),
                        schema,
                    )))
                } else {
                    Transformed::no(plan)
                })
            }
            node => Ok(Transformed::no(node)),
        }
    })
    .data()
}

fn materialize_in_expression(
    expr: Expr,
    outer: &mut Arc<LogicalPlan>,
    aliases: &AliasGenerator,
) -> Result<Transformed<Expr>> {
    expr.transform_up(|expr| {
        let Expr::Exists(mut exists) = expr else {
            return Ok(Transformed::no(expr));
        };
        let belongs_to_outer = |expr: &Expr| {
            matches!(expr, Expr::OuterReferenceColumn(_, column) if outer.schema().has_column(column))
        };
        if exists.subquery.outer_ref_columns.is_empty()
            || !exists.subquery.outer_ref_columns.iter().all(belongs_to_outer)
            || !exists.subquery.subquery.exists(|node| {
                Ok(matches!(node, LogicalPlan::Projection(project)
                    if project.expr.iter().any(Expr::contains_outer)))
            })?
        {
            return Ok(Transformed::no(Expr::Exists(exists)));
        }
        // Commit the outer projection only after the whole inner chain can retain its scopes.
        let mut bound_outer = Arc::clone(outer);
        let Some((inner, _)) = split_outer_columns(
            &exists.subquery.subquery,
            &HashSet::new(),
            &mut bound_outer,
            aliases,
        )? else {
            return Ok(Transformed::no(Expr::Exists(exists)));
        };
        exists.subquery.outer_ref_columns = inner.all_out_ref_exprs();
        exists.subquery.subquery = Arc::new(inner);
        *outer = bound_outer;
        Ok(Transformed::yes(Expr::Exists(exists)))
    })
}

fn expression_subquery(expr: &Expr) -> Option<&Subquery> {
    match expr {
        Expr::Exists(exists) => Some(&exists.subquery),
        Expr::ScalarSubquery(subquery) => Some(subquery),
        Expr::InSubquery(subquery) => Some(&subquery.subquery),
        _ => None,
    }
}

fn collect_input_columns(expr: &Expr, columns: &mut HashSet<Column>) -> Result<()> {
    columns.extend(expr.column_refs().into_iter().cloned());
    expr.apply(|expr| {
        if let Some(subquery) = expression_subquery(expr) {
            columns.extend(
                subquery
                    .outer_ref_columns
                    .iter()
                    .filter_map(|expr| match expr {
                        Expr::OuterReferenceColumn(_, column) => Some(column.clone()),
                        _ => None,
                    }),
            );
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(())
}

fn captures_remain_inner(expr: &Expr, schema: &DFSchema) -> Result<bool> {
    let mut kept = true;
    expr.apply(|expr| {
        if let Some(subquery) = expression_subquery(expr) {
            kept &= subquery.outer_ref_columns.iter().all(|expr| {
                matches!(expr, Expr::OuterReferenceColumn(_, column) if schema.has_column(column))
            });
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(kept)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CaptureOwner {
    Uncorrelated,
    Inner,
    Outer,
}

fn nested_queries_can_move(expr: &Expr) -> Result<bool> {
    let mut stable = true;
    expr.apply(|expr| {
        if let Some(subquery) = expression_subquery(expr) {
            stable &= captured_query_can_move(&subquery.subquery)?;
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(stable)
}

fn captured_query_can_move(plan: &LogicalPlan) -> Result<bool> {
    Ok(!plan.exists(|node| {
        if matches!(
            node,
            LogicalPlan::Limit(_)
                | LogicalPlan::Distinct(_)
                | LogicalPlan::Window(_)
                | LogicalPlan::Sort(_)
                | LogicalPlan::Extension(_)
        ) && !node.all_out_ref_exprs().is_empty()
        {
            return Ok(true);
        }
        for expr in node.expressions() {
            if expr.is_volatile() || contains_subquery(&expr)? {
                return Ok(true);
            }
        }
        Ok(false)
    })?)
}

fn rebind_captured_query(
    mut subquery: Subquery,
    inner: &DFSchema,
    removed: &HashMap<Column, Expr>,
) -> Result<Option<(Subquery, CaptureOwner)>> {
    let mut owner = CaptureOwner::Uncorrelated;
    for reference in &subquery.outer_ref_columns {
        let Expr::OuterReferenceColumn(_, column) = reference else {
            return Ok(None);
        };
        let side = if inner.has_column(column) {
            CaptureOwner::Inner
        } else if matches!(removed.get(column), Some(Expr::OuterReferenceColumn(_, _))) {
            CaptureOwner::Outer
        } else {
            return Ok(None);
        };
        if owner != CaptureOwner::Uncorrelated && owner != side {
            return Ok(None);
        }
        owner = side;
    }
    if owner == CaptureOwner::Outer {
        // Only this immediate scope moves. Deeper queries and volatile plans retain their producer.
        if !captured_query_can_move(&subquery.subquery)?
            || !subquery.subquery.all_out_ref_exprs().iter().all(|expr| {
                matches!(expr, Expr::OuterReferenceColumn(_, column) if removed.contains_key(column))
            })
        {
            return Ok(None);
        }
        let rebound = subquery
            .subquery
            .as_ref()
            .clone()
            .transform_up(|node| {
                node.map_expressions(|expr| {
                    expr.transform_up(|expr| {
                        if let Expr::OuterReferenceColumn(_, column) = &expr
                            && let Some(reference) = removed.get(column)
                        {
                            return Ok(Transformed::yes(reference.clone()));
                        }
                        Ok(Transformed::no(expr))
                    })
                })
            })?
            .data;
        subquery.outer_ref_columns = rebound.all_out_ref_exprs();
        subquery.subquery = Arc::new(rebound);
    }
    Ok(Some((subquery, owner)))
}

fn rebind_projection_captures(
    expr: Expr,
    inner: &DFSchema,
    removed: &HashMap<Column, Expr>,
) -> Result<Option<(Expr, CaptureOwner)>> {
    let mut owner = CaptureOwner::Uncorrelated;
    let mut valid = true;
    let expr = expr
        .transform_up(|expr| {
            let mut next_owner = CaptureOwner::Uncorrelated;
            let expr = match expr {
                Expr::Exists(mut exists) => {
                    if let Some((subquery, side)) =
                        rebind_captured_query(exists.subquery.clone(), inner, removed)?
                    {
                        exists.subquery = subquery;
                        next_owner = side;
                    } else {
                        valid = false;
                    }
                    Expr::Exists(exists)
                }
                Expr::ScalarSubquery(subquery) => {
                    if let Some((rebound, side)) =
                        rebind_captured_query(subquery.clone(), inner, removed)?
                    {
                        next_owner = side;
                        Expr::ScalarSubquery(rebound)
                    } else {
                        valid = false;
                        Expr::ScalarSubquery(subquery)
                    }
                }
                Expr::InSubquery(mut subquery) => {
                    if let Some((rebound, side)) =
                        rebind_captured_query(subquery.subquery.clone(), inner, removed)?
                    {
                        subquery.subquery = rebound;
                        next_owner = side;
                    } else {
                        valid = false;
                    }
                    Expr::InSubquery(subquery)
                }
                expr => expr,
            };
            if next_owner != CaptureOwner::Uncorrelated {
                if owner != CaptureOwner::Uncorrelated && owner != next_owner {
                    valid = false;
                }
                owner = next_owner;
            }
            Ok(Transformed::yes(expr))
        })?
        .data;
    Ok(valid.then_some((expr, owner)))
}

fn replace_removed_columns(expr: Expr, removed: &HashMap<Column, Expr>) -> Result<Expr> {
    expr.transform_up(|expr| {
        if let Expr::Column(column) = &expr
            && let Some(reference) = removed.get(column)
        {
            return Ok(Transformed::yes(reference.clone()));
        }
        Ok(Transformed::no(expr))
    })
    .data()
}

fn split_outer_columns(
    plan: &LogicalPlan,
    needed_columns: &HashSet<Column>,
    outer: &mut Arc<LogicalPlan>,
    aliases: &AliasGenerator,
) -> Result<Option<(LogicalPlan, HashMap<Column, Expr>)>> {
    if plan.all_out_ref_exprs().is_empty() {
        return Ok(Some((plan.clone(), HashMap::new())));
    }
    match plan {
        LogicalPlan::Projection(project) => {
            let mut needed_inputs = HashSet::new();
            for (expr, output) in project.expr.iter().zip(project.schema.columns()) {
                if !needed_columns.contains(&output) && !is_projection_leaf(expr) {
                    return Ok(None);
                }
            }
            for column in needed_columns {
                let index = project.schema.index_of_column(column)?;
                collect_input_columns(&project.expr[index], &mut needed_inputs)?;
            }
            let Some((input, removed)) =
                split_outer_columns(&project.input, &needed_inputs, outer, aliases)?
            else {
                return Ok(None);
            };
            let mut retained = Vec::new();
            let mut replacements = HashMap::new();
            for (expr, output) in project.expr.iter().zip(project.schema.columns()) {
                let expr = replace_removed_columns(expr.clone(), &removed)?;
                let Some((expr, capture_owner)) =
                    rebind_projection_captures(expr, input.schema(), &removed)?
                else {
                    return Ok(None);
                };
                if !expr.contains_outer() && capture_owner != CaptureOwner::Outer {
                    retained.push(expr);
                    continue;
                }
                if !expr.column_refs().is_empty()
                    || capture_owner == CaptureOwner::Inner
                    || !nested_queries_can_move(&expr)?
                    || expr.is_volatile()
                    || !find_out_reference_exprs(&expr).iter().all(|expr| {
                        matches!(expr, Expr::OuterReferenceColumn(_, column) if outer.schema().has_column(column))
                    })
                {
                    return Ok(None);
                }
                let mut value = expr;
                while let Expr::Alias(alias) = value {
                    value = *alias.expr;
                }
                let reference = if matches!(value, Expr::OuterReferenceColumn(_, _)) {
                    value
                } else {
                    let name = aliases.next("__spark_exists_binding");
                    let columns = outer.schema().columns().into_iter().map(Expr::Column);
                    let projected = LogicalPlanBuilder::from(Arc::clone(outer))
                        .project(columns.chain([strip_outer_reference(value).alias(&name)]))?
                        .build()?;
                    let field = projected
                        .schema()
                        .field_with_unqualified_name(&name)?
                        .clone();
                    *outer = Arc::new(projected);
                    Expr::OuterReferenceColumn(field, Column::new_unqualified(name))
                };
                replacements.insert(output, reference);
            }
            let inner = LogicalPlan::Projection(Projection::try_new(retained, Arc::new(input))?);
            Ok(Some((inner, replacements)))
        }
        LogicalPlan::SubqueryAlias(alias) => {
            let mut needed_inputs = HashSet::new();
            for column in needed_columns {
                let index = alias.schema.index_of_column(column)?;
                needed_inputs.insert(alias.input.schema().qualified_field(index).into());
            }
            let Some((input, removed)) =
                split_outer_columns(&alias.input, &needed_inputs, outer, aliases)?
            else {
                return Ok(None);
            };
            let replacements = alias
                .schema
                .columns()
                .into_iter()
                .zip(alias.input.schema().columns())
                .filter_map(|(output, input)| {
                    removed
                        .get(&input)
                        .cloned()
                        .map(|reference| (output, reference))
                })
                .collect();
            let inner = LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
                Arc::new(input),
                alias.alias.clone(),
            )?);
            Ok(Some((inner, replacements)))
        }
        LogicalPlan::Filter(filter) => {
            let mut needed_inputs = needed_columns.clone();
            collect_input_columns(&filter.predicate, &mut needed_inputs)?;
            let Some((input, removed)) =
                split_outer_columns(&filter.input, &needed_inputs, outer, aliases)?
            else {
                return Ok(None);
            };
            if !captures_remain_inner(&filter.predicate, input.schema())? {
                return Ok(None);
            }
            let predicate = replace_removed_columns(filter.predicate.clone(), &removed)?;
            let inner = LogicalPlan::Filter(Filter::try_new(predicate, Arc::new(input))?);
            Ok(Some((inner, removed)))
        }
        // A correlated row-changing operator needs the existing per-row adapter, not this split.
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use datafusion::arrow::array::{ArrayRef, Int32Array, RecordBatch};
    use datafusion::prelude::SessionContext;
    use datafusion_common::DataFusionError;

    use crate::config::PlanConfig;
    use crate::resolver::PlanResolver;

    async fn resolve(sql: &str) -> Result<LogicalPlan> {
        let session = SessionContext::new();
        for (name, fields) in [
            ("left_inputs", vec![("k", vec![2, 2]), ("n", vec![30, 31])]),
            (
                "right_inputs",
                vec![("k", vec![2, 2]), ("w", vec![200, 201])],
            ),
            ("allowed_keys", vec![("k", vec![200])]),
            ("outer_allowed_keys", vec![("k", vec![30])]),
        ] {
            session.register_batch(
                name,
                RecordBatch::try_from_iter(
                    fields.into_iter().map(|(name, values)| {
                        (name, Arc::new(Int32Array::from(values)) as ArrayRef)
                    }),
                )?,
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

    #[derive(Default)]
    struct PlanStructure {
        expression_nodes: usize,
        projection_count: usize,
        max_projection_width: usize,
        max_source_expression_nodes: usize,
        field_names: HashSet<String>,
    }

    fn plan_structure(plan: &LogicalPlan) -> Result<PlanStructure> {
        let mut structure = PlanStructure::default();
        plan.apply_with_subqueries(|node| {
            structure.field_names.extend(
                node.schema()
                    .fields()
                    .iter()
                    .map(|field| field.name().clone()),
            );
            if let LogicalPlan::Projection(project) = node {
                structure.projection_count += 1;
                structure.max_projection_width =
                    structure.max_projection_width.max(project.expr.len());
                for expr in &project.expr {
                    let mut count = 0;
                    expr.apply(|_| {
                        count += 1;
                        Ok(TreeNodeRecursion::Continue)
                    })?;
                    structure.max_source_expression_nodes =
                        structure.max_source_expression_nodes.max(count);
                }
            }
            node.apply_expressions(|expr| {
                expr.apply(|_| {
                    structure.expression_nodes += 1;
                    Ok(TreeNodeRecursion::Continue)
                })
            })?;
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(structure)
    }

    #[tokio::test]
    async fn materialized_chains_keep_one_producer_per_stage() -> Result<()> {
        // Each stage keeps w+i-1 columns and one producer; their sum is quadratic, not exponential.
        for depth in [4, 8] {
            let mut input = "SELECT l.k AS v,r.k AS inner_k FROM right_inputs r".to_string();
            for _ in 0..depth {
                input = format!("SELECT s.v+s.v AS v,s.inner_k FROM ({input}) s");
            }
            let before = resolve(&format!("SELECT l.k FROM left_inputs l WHERE EXISTS(SELECT 1 FROM ({input}) q WHERE q.v=q.inner_k)")).await?;
            let source = plan_structure(&before)?;
            let mut outer_width = 0;
            before.apply(|node| {
                if let LogicalPlan::Filter(filter) = node {
                    outer_width = filter.input.schema().fields().len();
                    return Ok(TreeNodeRecursion::Stop);
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
            assert!(outer_width > 0);
            let after = materialize_outer_exists_bindings(before, &AliasGenerator::new())?;
            let materialized = plan_structure(&after)?;
            let private = materialized
                .field_names
                .difference(&source.field_names)
                .cloned()
                .collect::<HashSet<_>>();
            assert_eq!(private.len(), depth);
            assert!(materialized.projection_count <= source.projection_count + depth + 1);
            assert!(materialized.max_projection_width <= outer_width + depth);
            let node_bound = source.expression_nodes
                + outer_width
                + depth * (outer_width + source.max_source_expression_nodes + 1)
                + depth * (depth - 1) / 2;
            assert!(materialized.expression_nodes <= node_bound, "depth={depth}");
            let mut definitions = HashMap::<String, usize>::new();
            after.apply_with_subqueries(|node| {
                if let LogicalPlan::Projection(project) = node {
                    for expr in &project.expr {
                        if let Expr::Alias(alias) = expr
                            && private.contains(&alias.name)
                            && !is_projection_leaf(&alias.expr)
                        {
                            *definitions.entry(alias.name.clone()).or_default() += 1;
                        }
                    }
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
            assert_eq!(definitions.len(), depth);
            assert!(definitions.values().all(|count| *count == 1));
            println!(
                "EXISTS_BINDING_GROWTH depth={depth} input_nodes={} output_nodes={} bound_nodes={node_bound} projections={} max_width={} private_producers={}",
                source.expression_nodes,
                materialized.expression_nodes,
                materialized.projection_count,
                materialized.max_projection_width,
                definitions.len()
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn one_sided_captures_bind_to_their_own_available_fields() -> Result<()> {
        for (payload, allowed) in [
            ("inner_payload", "allowed_keys"),
            ("outer_payload", "outer_allowed_keys"),
        ] {
            for flag in [
                format!("EXISTS(SELECT 1 FROM {allowed} t WHERE t.k=s.{payload})"),
                format!("s.{payload} IN(SELECT t.k FROM {allowed} t)"),
                format!("(SELECT MAX(t.k) FROM {allowed} t WHERE t.k=s.{payload})=s.{payload}"),
            ] {
                let before = resolve(&format!("SELECT l.k,l.n FROM left_inputs l WHERE EXISTS(SELECT 1 FROM (SELECT s.outer_k,s.inner_k,{flag} AS e FROM (SELECT l.k AS outer_k,l.n AS outer_payload,r.k AS inner_k,r.w AS inner_payload FROM right_inputs r) s) q WHERE q.outer_k=q.inner_k AND q.e)")).await?;
                let after = materialize_outer_exists_bindings(before, &AliasGenerator::new())?;
                assert!(!after.exists(|node| Ok(matches!(node, LogicalPlan::Projection(project) if project.expr.iter().any(Expr::contains_outer))))?);
                let mut query_count = 0;
                after.apply_with_subqueries(|node| {
                    if let Some(input) = node.inputs().first() {
                        node.apply_expressions(|expr| expr.apply(|expr| {
                            if let Some(query) = expression_subquery(expr) {
                                query_count += 1;
                                assert_eq!(query.outer_ref_columns, query.subquery.all_out_ref_exprs());
                                assert!(query.outer_ref_columns.iter().all(|expr| matches!(expr, Expr::OuterReferenceColumn(_, column) if input.schema().has_column(column))));
                            }
                            Ok(TreeNodeRecursion::Continue)
                        }))?;
                    }
                    Ok(TreeNodeRecursion::Continue)
                })?;
                assert_eq!(query_count, 2);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn unsafe_producers_and_correlated_barriers_keep_their_original_scope() -> Result<()> {
        for sql in [
            "SELECT l.k FROM left_inputs l WHERE EXISTS(SELECT 1 FROM (SELECT l.k+rand() AS outer_k,r.k AS inner_k FROM right_inputs r) q WHERE q.outer_k=q.inner_k AND q.outer_k>0)",
            "SELECT l.k FROM left_inputs l WHERE EXISTS(SELECT 1 FROM (SELECT s.outer_k,s.inner_k,EXISTS(SELECT 1 FROM outer_allowed_keys t WHERE t.k=s.outer_payload AND rand()>0.5) AS e FROM (SELECT l.k AS outer_k,l.n AS outer_payload,r.k AS inner_k FROM right_inputs r) s) q WHERE q.outer_k=q.inner_k AND q.e)",
            "SELECT l.k FROM left_inputs l WHERE EXISTS(SELECT 1 FROM (SELECT s.outer_k,s.inner_k,EXISTS(SELECT 1 FROM outer_allowed_keys t WHERE t.k=s.outer_payload AND t.k=s.inner_payload) AS e FROM (SELECT l.k+1 AS outer_k,l.n AS outer_payload,r.k AS inner_k,r.w AS inner_payload FROM right_inputs r) s) q WHERE q.outer_k=q.inner_k AND q.outer_k>0 AND q.e)",
            "SELECT l.k FROM left_inputs l WHERE EXISTS(SELECT 1 FROM (SELECT l.k+1 AS outer_k,r.k AS inner_k FROM right_inputs r LIMIT 1 OFFSET 1) q WHERE q.outer_k=q.inner_k AND q.outer_k>0)",
            "SELECT l.k FROM left_inputs l WHERE EXISTS(SELECT 1 FROM (SELECT l.k AS outer_k,r.k AS inner_k,raise_error(CAST(r.k AS STRING)) AS unused FROM right_inputs r) q WHERE q.outer_k=q.inner_k)",
        ] {
            let before = resolve(sql).await?;
            assert_eq!(
                materialize_outer_exists_bindings(before.clone(), &AliasGenerator::new())?,
                before
            );
        }
        let before = resolve("SELECT l.k FROM left_inputs l WHERE EXISTS(SELECT 1 FROM (SELECT s.outer_k,s.inner_k,EXISTS(SELECT 1 FROM outer_allowed_keys t WHERE t.k=s.outer_payload LIMIT 0) AS e FROM (SELECT l.k AS outer_k,l.n AS outer_payload,r.k AS inner_k FROM right_inputs r) s) q WHERE q.outer_k=q.inner_k AND q.e)").await?;
        let mut captured_limit_queries = 0;
        before.apply_with_subqueries(|node| {
            node.apply_expressions(|expr| {
                expr.apply(|expr| {
                    if let Some(query) = expression_subquery(expr)
                        && query.subquery.exists(|node| {
                            Ok(matches!(node, LogicalPlan::Limit(_))
                                && !node.all_out_ref_exprs().is_empty())
                        })?
                    {
                        captured_limit_queries += 1;
                        assert!(!captured_query_can_move(&query.subquery)?);
                    }
                    Ok(TreeNodeRecursion::Continue)
                })
            })?;
            Ok(TreeNodeRecursion::Continue)
        })?;
        assert_eq!(captured_limit_queries, 1);
        assert_eq!(
            materialize_outer_exists_bindings(before.clone(), &AliasGenerator::new())?,
            before
        );
        Ok(())
    }
}
