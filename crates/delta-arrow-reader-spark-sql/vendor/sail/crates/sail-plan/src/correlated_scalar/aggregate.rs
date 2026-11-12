use std::sync::Arc;

use datafusion::functions_aggregate::expr_fn::first_value;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{Column, Result, internal_err};
use datafusion_expr::expr_rewriter::{NamePreserver, SavedName, strip_outer_reference};
use datafusion_expr::utils::grouping_set_to_exprlist;
use datafusion_expr::{Aggregate, Expr, LogicalPlan, Projection, lit, when};

pub(super) fn can_lower_aggregate(aggregate: &Aggregate) -> bool {
    // A grouping set can create multiple groups from an unmatched row.
    !aggregate
        .group_expr
        .iter()
        .any(|expr| matches!(expr, Expr::GroupingSet(_)))
}

pub(super) fn aggregate_inner_rows(
    joined: LogicalPlan,
    aggregate: &Aggregate,
    having: Option<&Expr>,
    row_id: &Column,
    matched: &Column,
    outer_columns: &[Column],
) -> Result<LogicalPlan> {
    if !can_lower_aggregate(aggregate) {
        return internal_err!("cannot aggregate scalar subquery grouping sets after correlation");
    }
    let marker = Expr::Column(matched.clone());
    let is_match = marker.clone().is_not_null();
    let original_columns = aggregate.schema.columns();
    let keep_name = |expr: Expr, column: &Column| {
        SavedName::Saved {
            relation: column.relation.clone(),
            name: column.name.clone(),
        }
        .restore(expr)
    };

    let original_groups = grouping_set_to_exprlist(&aggregate.group_expr)?;
    let group_count = original_groups.len();
    let mut groups = vec![Expr::Column(row_id.clone())];
    for (expr, column) in original_groups.into_iter().zip(&original_columns) {
        let expr = strip_outer_reference(expr.clone());
        groups.push(keep_name(guard_unmatched(expr, &is_match)?, column));
    }
    let mut results = Vec::new();
    for (expr, column) in aggregate
        .aggr_expr
        .iter()
        .zip(&original_columns[group_count..])
    {
        let expr = strip_outer_reference(expr.clone())
            .transform_up(|expr| {
                let Expr::AggregateFunction(mut function) = expr else {
                    return Ok(Transformed::no(expr));
                };
                for arg in &mut function.params.args {
                    *arg = guard_unmatched(std::mem::take(arg), &is_match)?;
                }
                for sort in &mut function.params.order_by {
                    sort.expr = guard_unmatched(std::mem::take(&mut sort.expr), &is_match)?;
                }
                let filter = match function.params.filter.take() {
                    Some(filter) => when(is_match.clone(), *filter).otherwise(lit(false))?,
                    None => is_match.clone(),
                };
                function.params.filter = Some(Box::new(filter));
                Ok(Transformed::yes(Expr::AggregateFunction(function)))
            })
            .data()?;
        results.push(keep_name(expr, column));
    }
    results.extend(
        outer_columns
            .iter()
            .map(|column| keep_name(first_value(Expr::Column(column.clone()), vec![]), column)),
    );

    // A global aggregate returns one scalar row even on empty input. For a
    // grouped aggregate, the caller discards the unmatched row by its marker.
    let marker = if group_count == 0 {
        keep_name(lit(true), matched)
    } else {
        results.push(keep_name(first_value(marker.clone(), vec![]), matched));
        marker
    };
    let grouped = Aggregate::try_new(Arc::new(joined), groups, results)?;
    let projection = original_columns
        .into_iter()
        .chain(outer_columns.iter().cloned())
        .chain([row_id.clone()])
        .map(Expr::Column)
        .chain([marker])
        .collect();
    let rows = LogicalPlan::Projection(Projection::try_new(
        projection,
        Arc::new(LogicalPlan::Aggregate(grouped)),
    )?);
    if let Some(having) = having {
        // HAVING can reject the aggregate row. Retain it with a NULL marker
        // so the scalar still has an outer row to return NULL for.
        super::filter_match_marker(rows, matched, strip_outer_reference(having.clone()))
    } else {
        Ok(rows)
    }
}

// DataFusion evaluates aggregate arguments and keys before applying FILTER.
// Skip computations on the placeholder from the left join as well as its update.
fn guard_unmatched(expr: Expr, is_match: &Expr) -> Result<Expr> {
    match expr {
        Expr::Column(_) | Expr::Literal(_, _) => Ok(expr),
        Expr::Alias(mut alias) => {
            alias.expr = Box::new(guard_unmatched(*alias.expr, is_match)?);
            Ok(Expr::Alias(alias))
        }
        expr => when(is_match.clone(), expr).end(),
    }
}

pub(super) fn lift_correlated_scalar_aggregate_inputs(plan: LogicalPlan) -> Result<LogicalPlan> {
    Ok(plan
        .transform_up_with_subqueries(|node| {
            let names = NamePreserver::new(&node);
            let LogicalPlan::Aggregate(mut aggregate) = node else {
                return Ok(Transformed::no(node));
            };
            let input_schema = aggregate.input.schema();
            let mut lifted = Vec::new();
            let mut next = 0;
            for expr in &mut aggregate.aggr_expr {
                let name = names.save(expr);
                let mut body = &mut *expr;
                while let Expr::Alias(alias) = body {
                    body = &mut alias.expr;
                }
                let Expr::AggregateFunction(function) = body else {
                    continue;
                };
                // Lifting would evaluate arguments before an aggregate's own filter or order.
                if function.params.filter.is_some() || !function.params.order_by.is_empty() {
                    continue;
                }
                let before = lifted.len();
                for arg in &mut function.params.args {
                    if !arg.exists(|expr| {
                        Ok(match expr {
                            Expr::ScalarSubquery(subquery) => {
                                !subquery.outer_ref_columns.is_empty()
                            }
                            _ => false,
                        })
                    })? {
                        continue;
                    }
                    let alias = loop {
                        next += 1;
                        let alias = format!("__spark_scalar_arg_{next}");
                        if !input_schema
                            .fields()
                            .iter()
                            .any(|field| field.name() == &alias)
                        {
                            break alias;
                        }
                    };
                    let column = Expr::Column(Column::new_unqualified(&alias));
                    // Keep casts, metadata, and conditional evaluation with the scalar.
                    lifted.push(std::mem::replace(arg, column).alias(alias));
                }
                if lifted.len() != before {
                    *expr = name.restore(std::mem::take(expr));
                }
            }
            if lifted.is_empty() {
                return Ok(Transformed::no(LogicalPlan::Aggregate(aggregate)));
            }
            // DataFusion decorrelates scalars in Projection. Preserve the input
            // qualifiers so both correlation keys and other consumers still bind.
            let mut projection: Vec<_> = input_schema
                .columns()
                .into_iter()
                .map(Expr::Column)
                .collect();
            projection.extend(lifted);
            aggregate.input = Arc::new(LogicalPlan::Projection(Projection::try_new(
                projection,
                aggregate.input,
            )?));
            Ok(Transformed::yes(LogicalPlan::Aggregate(aggregate)))
        })?
        .data)
}
