use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::functions_window::expr_fn::row_number;
use datafusion_common::{Column, Result, ScalarValue, internal_err};
use datafusion_expr::expr_rewriter::strip_outer_reference;
use datafusion_expr::{Expr, ExprFunctionExt, Limit, LogicalPlan, Sort, Window, lit, when};

use super::filter_match_marker;

fn literal_bound(expr: Option<&Expr>, default: u64) -> Option<u64> {
    let Some(expr) = expr else {
        return Some(default);
    };
    // DataFusion can widen a Spark integer literal before folding the cast.
    let expr = match expr {
        Expr::Cast(cast) if cast.field.data_type() == &DataType::Int64 => cast.expr.as_ref(),
        expr => expr,
    };
    match expr {
        Expr::Literal(ScalarValue::Int32(Some(value)), _) => u64::try_from(*value).ok(),
        Expr::Literal(ScalarValue::Int64(Some(value)), _) => u64::try_from(*value).ok(),
        Expr::Literal(
            ScalarValue::Int32(None) | ScalarValue::Int64(None) | ScalarValue::Null,
            _,
        ) => Some(default),
        _ => None,
    }
}

fn literal_bounds(limit: &Limit) -> Option<(u64, u64)> {
    Some((
        literal_bound(limit.skip.as_deref(), 0)?,
        literal_bound(limit.fetch.as_deref(), u64::MAX)?,
    ))
}

pub(super) fn can_lower_limit(limit: &Limit) -> bool {
    literal_bounds(limit).is_some()
}

pub(super) fn limit_inner_rows(
    joined: LogicalPlan,
    limit: &Limit,
    sort: Option<&Sort>,
    row_id: &Column,
    matched: &Column,
) -> Result<LogicalPlan> {
    let Some((skip, fetch)) = literal_bounds(limit) else {
        return internal_err!("Correlated scalar limit requires literal OFFSET and LIMIT");
    };
    let end = skip.saturating_add(fetch).min(
        sort.and_then(|sort| sort.fetch)
            .map_or(u64::MAX, |n| n as u64),
    );
    if end <= skip {
        return filter_match_marker(joined, matched, lit(false));
    }
    let position = Column::new_unqualified(format!("{}_position", row_id.name));
    let mut order = sort.map(|sort| sort.expr.clone()).unwrap_or_default();
    for sort in &mut order {
        let key = strip_outer_reference(std::mem::take(&mut sort.expr));
        sort.expr = match key {
            Expr::Column(_) | Expr::Literal(_, _) => key,
            key => when(Expr::Column(matched.clone()).is_not_null(), key).end()?,
        };
    }
    let numbered = row_number()
        .partition_by(vec![Expr::Column(row_id.clone())])
        .order_by(order)
        .build()?
        .alias(&position.name);
    let ranked = LogicalPlan::Window(Window::try_new(vec![numbered], Arc::new(joined))?);

    // Keep even out-of-range rows so each outer row still has a group.
    let position = Expr::Column(position);
    filter_match_marker(
        ranked,
        matched,
        position.clone().gt(lit(skip)).and(position.lt_eq(lit(end))),
    )
}
