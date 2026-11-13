use std::collections::BTreeSet;
use std::sync::Arc;

use datafusion::arrow::datatypes::DataType;
use datafusion::functions_aggregate::expr_fn::{count, first_value};
use datafusion::optimizer::AnalyzerRule;
use datafusion::optimizer::decorrelate::PullUpCorrelatedExpr;
use datafusion_common::alias::AliasGenerator;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{Column, Result, ScalarValue};
use datafusion_expr::expr_rewriter::{NamePreserver, replace_col, strip_outer_reference};
use datafusion_expr::logical_plan::{Extension, JoinType, Projection, Subquery};
use datafusion_expr::{
    Expr, ExprFunctionExt, LogicalPlan, LogicalPlanBuilder, ScalarUDF, cast, lit, when,
};
use sail_function::scalar::misc::raise_error::RaiseError;
use sail_logical_plan::monotonic_id::{MonotonicIdNode, MonotonicIdPurpose};

use crate::expression_errors::SparkSqlExpressionSemantics;

mod aggregate;
mod exists;
mod limit;
mod rows;

use aggregate::lift_correlated_scalar_aggregate_inputs;
use exists::inline_projected_outer_exists;
use rows::{
    apply_row_steps, has_correlated_self_equality, move_correlated_inner_join_predicates,
    split_correlated_rows, split_scalar_filter_predicates,
};

/// Bind projected-outer EXISTS and lower Spark scalars before DataFusion validates them.
#[derive(Debug)]
pub struct LowerSparkCorrelatedSubqueries;

impl AnalyzerRule for LowerSparkCorrelatedSubqueries {
    fn name(&self) -> &str {
        "lower_spark_correlated_subqueries"
    }

    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        if config
            .extensions
            .get::<SparkSqlExpressionSemantics>()
            .is_none()
        {
            return Ok(plan);
        }
        lower_correlated_scalar_subqueries(
            lift_correlated_scalar_aggregate_inputs(inline_projected_outer_exists(plan)?)?,
            &AliasGenerator::new(),
            None,
        )
    }
}

fn lower_correlated_scalar_subqueries(
    plan: LogicalPlan,
    aliases: &AliasGenerator,
    match_marker: Option<&Column>,
) -> Result<LogicalPlan> {
    // Split source filters before postorder traversal can lower their nested scalars.
    split_scalar_filter_predicates(plan)?
        .transform_up_with_subqueries(|node| {
            let names = NamePreserver::new(&node);
            match node {
                // Bind ancestor rows before lowering scalars that depend on those rows.
                LogicalPlan::Projection(mut projection)
                    if projection.expr.iter().any(Expr::contains_scalar_subquery)
                        && projection.input.all_out_ref_exprs().is_empty() =>
                {
                    let mut changed = false;
                    for expr in &mut projection.expr {
                        if expr.contains_scalar_subquery() {
                            let name = names.save(expr);
                            let lowered = lower_expression(
                                expr.clone(),
                                &mut projection.input,
                                aliases,
                                match_marker,
                            )?;
                            changed |= lowered.transformed;
                            *expr = name.restore(lowered.data);
                        }
                    }
                    Ok(Transformed::new_transformed(
                        LogicalPlan::Projection(projection),
                        changed,
                    ))
                }
                LogicalPlan::Filter(mut filter)
                    if filter.predicate.contains_scalar_subquery()
                        && filter.input.all_out_ref_exprs().is_empty() =>
                {
                    let schema = Arc::clone(filter.input.schema());
                    let lowered = lower_expression(
                        filter.predicate,
                        &mut filter.input,
                        aliases,
                        match_marker,
                    )?;
                    filter.predicate = lowered.data;
                    let node = LogicalPlan::Filter(filter);
                    if lowered.transformed {
                        Ok(Transformed::yes(LogicalPlan::Projection(
                            Projection::new_from_schema(Arc::new(node), schema),
                        )))
                    } else {
                        Ok(Transformed::no(node))
                    }
                }
                node => Ok(Transformed::no(node)),
            }
        })
        .data()
}

fn lower_expression(
    expr: Expr,
    input: &mut Arc<LogicalPlan>,
    aliases: &AliasGenerator,
    match_marker: Option<&Column>,
) -> Result<Transformed<Expr>> {
    expr.transform_up(|expr| {
        if let Expr::ScalarSubquery(subquery) = &expr
            && let Some((plan, value)) = lower_scalar(subquery, input, aliases, match_marker)?
        {
            *input = Arc::new(plan);
            return Ok(Transformed::yes(value));
        }
        Ok(Transformed::no(expr))
    })
}

fn contains_subquery(expr: &Expr) -> Result<bool> {
    expr.exists(|expr| {
        Ok(matches!(
            expr,
            Expr::ScalarSubquery(_) | Expr::Exists(_) | Expr::InSubquery(_)
        ))
    })
}

fn is_projection_leaf(mut expr: &Expr) -> bool {
    while let Expr::Alias(alias) = expr {
        expr = &alias.expr;
    }
    matches!(
        expr,
        Expr::Column(_) | Expr::OuterReferenceColumn(_, _) | Expr::Literal(_, _)
    )
}

fn inline_projection_or_alias(
    value: Expr,
    input: &LogicalPlan,
) -> Result<Option<(Expr, &LogicalPlan)>> {
    // Nested queries capture these scopes; expression substitution cannot rebind them.
    if contains_subquery(&value)? {
        return Ok(None);
    }
    match input {
        LogicalPlan::Projection(project) => {
            let mut computations = BTreeSet::new();
            let mut shared_computation = false;
            let rewritten = value
                .transform_up(|expr| {
                    let Expr::Column(column) = &expr else {
                        return Ok(Transformed::no(expr));
                    };
                    let index = project.schema.index_of_column(column)?;
                    let source = &project.expr[index];
                    shared_computation |=
                        !is_projection_leaf(source) && !computations.insert(index);
                    if shared_computation {
                        return Ok(Transformed::no(expr));
                    }
                    Ok(Transformed::yes(source.clone().unalias()))
                })?
                .data;
            // Shared producers stay in their projection, avoiding duplicated work and tree growth.
            if shared_computation {
                Ok(None)
            } else {
                Ok(Some((rewritten, project.input.as_ref())))
            }
        }
        LogicalPlan::SubqueryAlias(alias) => {
            let rewritten = value
                .transform_up(|expr| {
                    let Expr::Column(column) = &expr else {
                        return Ok(Transformed::no(expr));
                    };
                    let index = alias.schema.index_of_column(column)?;
                    let column = alias.input.schema().qualified_field(index).into();
                    Ok(Transformed::yes(Expr::Column(column)))
                })?
                .data;
            Ok(Some((rewritten, alias.input.as_ref())))
        }
        _ => Ok(None),
    }
}

fn inline_projection_bindings(
    mut value: Expr,
    mut input: &LogicalPlan,
) -> Result<(Expr, &LogicalPlan)> {
    while let Some((rebound, remaining)) = inline_projection_or_alias(value.clone(), input)? {
        value = rebound;
        input = remaining;
    }
    Ok((value, input))
}

fn clear_match_marker_on_rejection(
    plan: LogicalPlan,
    marker: &Column,
    predicate: Expr,
) -> Result<LogicalPlan> {
    let null = lit(ScalarValue::Boolean(None));
    let retained = when(Expr::Column(marker.clone()).is_null(), null.clone())
        .when(predicate, lit(true))
        .otherwise(null)?
        .alias_qualified(marker.relation.clone(), &marker.name);
    let columns = plan.schema().columns().into_iter().map(|column| {
        if &column == marker {
            retained.clone()
        } else {
            Expr::Column(column)
        }
    });
    let columns = columns.collect::<Vec<_>>();
    LogicalPlanBuilder::from(plan).project(columns)?.build()
}

fn lower_scalar(
    subquery: &Subquery,
    outer: &Arc<LogicalPlan>,
    aliases: &AliasGenerator,
    outer_match_marker: Option<&Column>,
) -> Result<Option<(LogicalPlan, Expr)>> {
    let outer_match_marker = outer_match_marker.filter(|marker| outer.schema().has_column(marker));
    let belongs_to_outer = |expr: &Expr| matches!(expr, Expr::OuterReferenceColumn(_, col) if outer.schema().has_column(col));
    if subquery.outer_ref_columns.is_empty()
        || !subquery.outer_ref_columns.iter().all(belongs_to_outer)
        || !subquery
            .subquery
            .all_out_ref_exprs()
            .iter()
            .all(belongs_to_outer)
    {
        return Ok(None);
    }
    let schema = subquery.subquery.schema();
    if schema.fields().len() != 1 {
        return Ok(None);
    }
    let output = Expr::Column(schema.qualified_field(0).into());
    let inner = move_correlated_inner_join_predicates(subquery.subquery.as_ref().clone())?;
    let inner = split_scalar_filter_predicates(inner)?;
    let (value, input) = inline_projection_bindings(output, &inner)?;
    // Nested aggregates must consume this level's matches before scalar grouping.
    if outer_match_marker.is_none()
        && let LogicalPlan::Aggregate(aggregate) = input
        && aggregate.group_expr.is_empty()
        && !has_correlated_self_equality(input)?
        && !input.exists(|node| {
            Ok(matches!(node, LogicalPlan::Window(_)) && !node.all_out_ref_exprs().is_empty())
        })?
    {
        // Keep DataFusion's existing global aggregate decorrelation when it can
        // group on the extracted join keys before joining to the outer rows.
        let mut native = PullUpCorrelatedExpr::new();
        if subquery
            .subquery
            .as_ref()
            .clone()
            .rewrite(&mut native)
            .is_ok()
            && native.can_pull_up
            && native.pulled_up_scalar_agg
        {
            return Ok(None);
        }
    }
    if matches!(input, LogicalPlan::EmptyRelation(empty) if empty.produce_one_row) {
        return Ok(None);
    }
    let Some(rows) = split_correlated_rows(input, aliases)? else {
        return Ok(None);
    };
    // Evaluate the selected value only when the subquery returns a row.
    let value = strip_outer_reference(value);

    let row_id = Column::new_unqualified(aliases.next("__spark_scalar_row"));
    let match_marker = Column::new_unqualified(aliases.next("__spark_scalar_match"));
    let outer_columns = outer.schema().columns();
    let inner_columns = rows.input.schema().columns();
    let left = LogicalPlan::Extension(Extension {
        node: Arc::new(MonotonicIdNode::try_new(
            Arc::clone(outer),
            row_id.name.clone(),
            MonotonicIdPurpose::ScalarCorrelation,
        )?),
    });
    let right = LogicalPlanBuilder::from(rows.input)
        .project(
            inner_columns
                .into_iter()
                .map(Expr::Column)
                .chain([lit(true).alias(&match_marker.name)]),
        )?
        .build()?;
    let join_filter = match outer_match_marker {
        Some(marker) => rows
            .join_filter
            .and(Expr::Column(marker.clone()).is_not_null()),
        None => rows.join_filter,
    };
    let joined = LogicalPlanBuilder::from(left)
        .join_on(right, JoinType::Left, [join_filter])?
        .build()?;
    let rows = apply_row_steps(joined, rows.steps, &row_id, &match_marker, &outer_columns)?;
    let mut rows = Arc::new(lower_correlated_scalar_subqueries(
        rows,
        aliases,
        Some(&match_marker),
    )?);
    let value = lower_expression(value, &mut rows, aliases, Some(&match_marker))?.data;
    let scalar_input = rows.schema();

    let match_count = Column::new_unqualified(aliases.next("__spark_scalar_count"));
    let mut referenced = value.column_refs().into_iter().collect::<Vec<_>>();
    referenced.sort_unstable();
    let mut renamed = Vec::new();
    let mut aggregates = Vec::new();
    for column in outer_columns {
        let expr = first_value(Expr::Column(column.clone()), vec![]);
        aggregates.push(expr.alias_qualified(column.relation, column.name));
    }
    for column in referenced {
        if let Ok(index) = scalar_input.index_of_column(column) {
            let inner_column = scalar_input.qualified_field(index).into();
            let name = aliases.next("__spark_scalar_value");
            let expr = first_value(Expr::Column(inner_column), vec![])
                .filter(Expr::Column(match_marker.clone()).is_not_null())
                .build()?;
            aggregates.push(expr.alias(&name));
            renamed.push(((*column).clone(), Column::new_unqualified(name)));
        } else if !outer.schema().has_column(column) {
            return Ok(None);
        }
    }
    let replacements = renamed.iter().map(|(old, new)| (old, new)).collect();
    let value = replace_col(value, &replacements)?;
    aggregates.push(count(Expr::Column(match_marker.clone())).alias(&match_count.name));

    let grouped = LogicalPlanBuilder::from(Arc::unwrap_or_clone(rows))
        .aggregate(vec![Expr::Column(row_id)], aggregates)?
        // Spark checks scalar cardinality before evaluating its consumer,
        // including when a CASE branch does not use the result.
        .filter(
            when(
                Expr::Column(match_count.clone()).gt(lit(1i64)),
                cast(
                    ScalarUDF::from(RaiseError::new())
                        .call(vec![lit("Scalar subquery returned more than one row")]),
                    DataType::Boolean,
                ),
            )
            .otherwise(lit(true))?,
        )?
        .build()?;

    let data_type = schema.field(0).data_type();
    let value = when(Expr::Column(match_count).eq(lit(1i64)), value)
        .otherwise(lit(ScalarValue::try_new_null(data_type)?))?;
    Ok(Some((grouped, value)))
}
