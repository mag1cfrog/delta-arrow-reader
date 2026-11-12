use std::sync::Arc;

use datafusion::functions_aggregate::expr_fn::{count, first_value};
use datafusion::optimizer::AnalyzerRule;
use datafusion::optimizer::decorrelate::PullUpCorrelatedExpr;
use datafusion_common::alias::AliasGenerator;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{Column, Result, ScalarValue};
use datafusion_expr::expr_rewriter::{NamePreserver, replace_col, strip_outer_reference};
use datafusion_expr::logical_plan::{Extension, JoinType, Projection, Subquery};
use datafusion_expr::utils::split_conjunction;
use datafusion_expr::{
    Expr, LogicalPlan, LogicalPlanBuilder, Operator, ScalarUDF, cast, lit, when,
};
use sail_function::scalar::misc::raise_error::RaiseError;
use sail_logical_plan::monotonic_id::{MonotonicIdNode, MonotonicIdPurpose};

use crate::expression_errors::{
    SparkSqlExpressionSemantics, lift_correlated_scalar_aggregate_inputs,
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
    let LogicalPlan::Projection(project) = subquery.subquery.as_ref() else {
        return Ok(None);
    };
    let [value] = project.expr.as_slice() else {
        return Ok(None);
    };
    if project.input.exists(|node| {
        Ok(match node {
            LogicalPlan::Projection(_)
            | LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::TableScan(_)
            | LogicalPlan::Values(_) => false,
            LogicalPlan::EmptyRelation(empty) => empty.produce_one_row,
            // DataFusion's pull-up discards self-equalities, even on NULL.
            LogicalPlan::Filter(filter) => {
                split_conjunction(&filter.predicate)
                    .into_iter()
                    .any(|predicate| {
                        matches!(predicate, Expr::BinaryExpr(binary)
                    if binary.op == Operator::Eq && binary.left == binary.right
                        && predicate.contains_outer())
                    })
            }
            _ => true,
        })
    })? {
        return Ok(None);
    }

    // Count matches before evaluating the result projection, which can depend
    // on the outer row and must only run when the subquery returns a row.
    let value = strip_outer_reference(value.clone().unalias());
    let mut pull_up = PullUpCorrelatedExpr::new();
    let inner = project
        .input
        .as_ref()
        .clone()
        .rewrite(&mut pull_up)
        .data()?;
    if !pull_up.can_pull_up {
        return Ok(None);
    }

    let row_id = Column::new_unqualified(aliases.next("__spark_scalar_row"));
    let matched = Column::new_unqualified(aliases.next("__spark_scalar_match"));
    let matches = Column::new_unqualified(aliases.next("__spark_scalar_count"));
    let mut referenced = value.column_refs().into_iter().collect::<Vec<_>>();
    referenced.sort_unstable();
    let inner_columns = inner.schema().columns();
    let mut renamed = Vec::new();
    let mut aggregates = Vec::new();
    for column in outer.schema().columns() {
        let expr = first_value(Expr::Column(column.clone()), vec![]);
        aggregates.push(expr.alias_qualified(column.relation, column.name));
    }
    for column in referenced {
        if let Ok(index) = inner.schema().index_of_column(column) {
            let name = aliases.next("__spark_scalar_value");
            let expr = first_value(Expr::Column(inner_columns[index].clone()), vec![]);
            aggregates.push(expr.alias(&name));
            renamed.push(((*column).clone(), Column::new_unqualified(name)));
        } else if !outer.schema().has_column(column) {
            return Ok(None);
        }
    }
    let replacements = renamed.iter().map(|(old, new)| (old, new)).collect();
    let value = replace_col(value, &replacements)?;
    aggregates.push(count(Expr::Column(matched.clone())).alias(&matches.name));

    let left = LogicalPlan::Extension(Extension {
        node: Arc::new(MonotonicIdNode::try_new(
            Arc::clone(outer),
            row_id.name.clone(),
            MonotonicIdPurpose::ScalarCorrelation,
        )?),
    });
    let right = LogicalPlanBuilder::from(inner)
        .project(
            inner_columns
                .into_iter()
                .map(Expr::Column)
                .chain([lit(true).alias(&matched.name)]),
        )?
        .build()?;
    let grouped = LogicalPlanBuilder::from(left)
        .join_on(right, JoinType::Left, pull_up.join_filters)?
        .aggregate(vec![Expr::Column(row_id)], aggregates)?
        .build()?;

    // The marker distinguishes an unmatched row from a real NULL. Checking
    // cardinality at the original use site preserves conditional evaluation.
    let data_type = subquery.subquery.schema().field(0).data_type();
    let matches = Expr::Column(matches);
    let too_many = ScalarUDF::from(RaiseError::new())
        .call(vec![lit("Scalar subquery returned more than one row")]);
    let value = when(
        matches.clone().gt(lit(1i64)),
        cast(too_many, data_type.clone()),
    )
    .when(matches.eq(lit(1i64)), value)
    .otherwise(lit(ScalarValue::try_new_null(data_type)?))?;
    Ok(Some((grouped, value)))
}
