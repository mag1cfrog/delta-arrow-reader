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
mod limit;
mod rows;

use aggregate::lift_correlated_scalar_aggregate_inputs;
use rows::{
    apply_row_steps, has_correlated_self_equality, move_correlated_inner_join_predicates,
    split_correlated_rows,
};

/// Lower Spark scalar queries over real inner rows before DataFusion validates them.
#[derive(Debug)]
pub struct LowerSparkCorrelatedScalars;

impl AnalyzerRule for LowerSparkCorrelatedScalars {
    fn name(&self) -> &str {
        "lower_spark_correlated_scalars"
    }

    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        if config
            .extensions
            .get::<SparkSqlExpressionSemantics>()
            .is_none()
        {
            return Ok(plan);
        }
        lower_correlated_scalar_subqueries(lift_correlated_scalar_aggregate_inputs(plan)?)
    }
}

fn lower_correlated_scalar_subqueries(plan: LogicalPlan) -> Result<LogicalPlan> {
    let aliases = AliasGenerator::new();
    plan.transform_up_with_subqueries(|node| {
        let names = NamePreserver::new(&node);
        match node {
            LogicalPlan::Projection(mut projection) => {
                let mut changed = false;
                for expr in &mut projection.expr {
                    if expr.contains_scalar_subquery() {
                        let name = names.save(expr);
                        let lowered =
                            lower_expression(expr.clone(), &mut projection.input, &aliases)?;
                        changed |= lowered.transformed;
                        *expr = name.restore(lowered.data);
                    }
                }
                Ok(Transformed::new_transformed(
                    LogicalPlan::Projection(projection),
                    changed,
                ))
            }
            LogicalPlan::Filter(mut filter) if filter.predicate.contains_scalar_subquery() => {
                let schema = Arc::clone(filter.input.schema());
                let lowered = lower_expression(filter.predicate, &mut filter.input, &aliases)?;
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
) -> Result<Transformed<Expr>> {
    expr.transform_up(|expr| {
        if let Expr::ScalarSubquery(subquery) = &expr
            && let Some((plan, value)) = lower_scalar(subquery, input, aliases)?
        {
            *input = Arc::new(plan);
            return Ok(Transformed::yes(value));
        }
        Ok(Transformed::no(expr))
    })
}

fn scalar_value_input(mut value: Expr, mut input: &LogicalPlan) -> Result<(Expr, &LogicalPlan)> {
    // Rebuild the selected expression through leading projections and aliases.
    // Leave a shared volatile output in its projection so it still runs once.
    loop {
        match input {
            LogicalPlan::Projection(project) => {
                let mut volatile = BTreeSet::new();
                let mut shared_volatile = false;
                let rewritten = value
                    .clone()
                    .transform_up(|expr| {
                        let Expr::Column(column) = &expr else {
                            return Ok(Transformed::no(expr));
                        };
                        let index = project.schema.index_of_column(column)?;
                        let source = &project.expr[index];
                        shared_volatile |= source.is_volatile() && !volatile.insert(index);
                        Ok(Transformed::yes(source.clone().unalias()))
                    })?
                    .data;
                if shared_volatile {
                    break;
                }
                value = rewritten;
                input = &project.input;
            }
            LogicalPlan::SubqueryAlias(alias) => {
                value = value
                    .transform_up(|expr| {
                        let Expr::Column(column) = &expr else {
                            return Ok(Transformed::no(expr));
                        };
                        let index = alias.schema.index_of_column(column)?;
                        let column = alias.input.schema().qualified_field(index).into();
                        Ok(Transformed::yes(Expr::Column(column)))
                    })?
                    .data;
                input = &alias.input;
            }
            _ => break,
        }
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
) -> Result<Option<(LogicalPlan, Expr)>> {
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
    let (value, input) = scalar_value_input(output, &inner)?;
    if let LogicalPlan::Aggregate(aggregate) = input
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
    let scalar_input = input.schema();

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
    let joined = LogicalPlanBuilder::from(left)
        .join_on(right, JoinType::Left, [rows.join_filter])?
        .build()?;
    let rows = apply_row_steps(joined, rows.steps, &row_id, &match_marker, &outer_columns)?;

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

    let grouped = LogicalPlanBuilder::from(rows)
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
