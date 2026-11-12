use std::sync::Arc;

use datafusion::optimizer::decorrelate::PullUpCorrelatedExpr;
use datafusion_common::alias::AliasGenerator;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, DFSchema, Result, ScalarValue, internal_err};
use datafusion_expr::expr_rewriter::{NamePreserver, strip_outer_reference};
use datafusion_expr::utils::{conjunction, disjunction, split_conjunction};
use datafusion_expr::{
    Aggregate, Distinct, Expr, Extension, Filter, JoinType, Limit, LogicalPlan, LogicalPlanBuilder,
    Operator, Projection, Sort, SubqueryAlias, Union, Window, WindowFunctionDefinition, lit, when,
};
use sail_logical_plan::monotonic_id::{MonotonicIdNode, MonotonicIdPurpose};
use sail_logical_plan::sort::RequiredSortNode;

use super::aggregate::{
    aggregate_inner_rows, can_lower_aggregate, distinct_inner_rows, guard_unmatched,
};
use super::clear_match_marker_on_rejection;
use super::limit::{can_lower_limit, limit_inner_rows};

pub(super) struct CorrelatedRows<'a> {
    pub input: LogicalPlan,
    pub join_filter: Expr,
    pub steps: Vec<RowStep<'a>>,
}

pub(super) enum RowStep<'a> {
    Project(&'a Projection),
    Alias(&'a SubqueryAlias),
    Filter(&'a Expr),
    Aggregate(&'a Aggregate),
    Distinct(&'a Arc<LogicalPlan>),
    Limit(&'a Limit, Option<Sort>),
    Window(&'a Window),
    Sort(&'a Sort),
    WindowOrder(&'a Extension),
    UnionOutput(Vec<Expr>, &'a DFSchema),
}

pub(super) fn split_correlated_rows<'a>(
    mut input: &'a LogicalPlan,
    aliases: &AliasGenerator,
) -> Result<Option<CorrelatedRows<'a>>> {
    let mut steps = Vec::new();
    loop {
        if can_pull_up(input)? {
            let mut pull_up = PullUpCorrelatedExpr::new();
            let inner = preserve_self_equalities(input.clone())?
                .rewrite(&mut pull_up)
                .data()?;
            if pull_up.can_pull_up {
                // Correlation can appear only in the selected value.
                let join_filter = conjunction(pull_up.join_filters).unwrap_or_else(|| lit(true));
                return Ok(Some(CorrelatedRows {
                    input: inner,
                    join_filter,
                    steps,
                }));
            }
        }
        // Keep row-changing operations on the per-outer-row side of the join.
        input = match input {
            LogicalPlan::Projection(project) => {
                steps.push(RowStep::Project(project));
                &project.input
            }
            LogicalPlan::SubqueryAlias(alias) => {
                steps.push(RowStep::Alias(alias));
                &alias.input
            }
            LogicalPlan::Filter(filter) => {
                steps.push(RowStep::Filter(&filter.predicate));
                &filter.input
            }
            LogicalPlan::Aggregate(aggregate) if can_lower_aggregate(aggregate) => {
                steps.push(RowStep::Aggregate(aggregate));
                &aggregate.input
            }
            LogicalPlan::Distinct(Distinct::All(input)) if !input.schema().fields().is_empty() => {
                steps.push(RowStep::Distinct(input));
                input
            }
            LogicalPlan::Limit(limit) if can_lower_limit(limit) => split_limit(limit, &mut steps),
            LogicalPlan::Window(window) => {
                steps.push(RowStep::Window(window));
                &window.input
            }
            LogicalPlan::Extension(extension)
                if extension
                    .node
                    .as_any()
                    .downcast_ref::<MonotonicIdNode>()
                    .is_some_and(|node| node.purpose() == MonotonicIdPurpose::WindowOrdering)
                    || extension
                        .node
                        .as_any()
                        .downcast_ref::<RequiredSortNode>()
                        .is_some_and(|node| node.fetch().is_none()) =>
            {
                steps.push(RowStep::WindowOrder(extension));
                extension.node.inputs()[0]
            }
            LogicalPlan::Union(union) => return split_union_rows(union, steps, aliases),
            LogicalPlan::Sort(sort)
                if sort.fetch.is_none()
                    && steps.iter().all(|step| {
                        matches!(
                            step,
                            RowStep::Project(_) | RowStep::Alias(_) | RowStep::Filter(_)
                        )
                    }) =>
            {
                // A scalar observes at most one row unless another operator consumes the order.
                &sort.input
            }
            LogicalPlan::Sort(sort) if sort.fetch.is_none() => {
                steps.push(RowStep::Sort(sort));
                &sort.input
            }
            _ => return Ok(None),
        };
    }
}

fn split_union_rows<'a>(
    union: &'a Union,
    mut steps: Vec<RowStep<'a>>,
    aliases: &AliasGenerator,
) -> Result<Option<CorrelatedRows<'a>>> {
    let mut branches = Vec::new();
    for input in &union.inputs {
        let Some(rows) = split_correlated_rows(input, aliases)? else {
            return Ok(None);
        };
        if !rows.input.all_out_ref_exprs().is_empty() {
            return Ok(None);
        }
        branches.push(rows);
    }

    // Branches keep private columns because their join keys and predicates can differ.
    let sources = branches
        .iter()
        .flat_map(|branch| {
            branch.input.schema().fields().iter().map(|field| {
                (
                    Column::new_unqualified(aliases.next("__spark_scalar_union")),
                    field.data_type().clone(),
                )
            })
        })
        .collect::<Vec<_>>();
    let tag = Column::new_unqualified(aliases.next("__spark_scalar_branch"));
    let mut inputs = Vec::new();
    let mut filters = Vec::new();
    let mut output = union
        .schema
        .fields()
        .iter()
        .map(|field| Ok(lit(ScalarValue::try_new_null(field.data_type())?)))
        .collect::<Result<Vec<_>>>()?;
    let mut offset = 0;
    for (index, branch) in branches.into_iter().enumerate() {
        let mut schema = branch.input.schema().as_ref();
        let columns = schema.columns();
        let mut values = sources[offset..offset + columns.len()]
            .iter()
            .map(|(column, _)| Expr::Column(column.clone()))
            .collect::<Vec<_>>();
        let mut filter = inline_union_columns(branch.join_filter, schema, &values)?;
        for step in branch.steps.into_iter().rev() {
            // Keep a computed volatile value materialized before another consumer.
            if values.iter().any(Expr::is_volatile) && !matches!(step, RowStep::Alias(_)) {
                return Ok(None);
            }
            match step {
                RowStep::Project(project) => {
                    values = project
                        .expr
                        .iter()
                        .map(|expr| inline_union_columns(expr.clone().unalias(), schema, &values))
                        .collect::<Result<_>>()?;
                    schema = &project.schema;
                }
                RowStep::Alias(alias) => {
                    values = alias
                        .input
                        .schema()
                        .columns()
                        .iter()
                        .map(|column| Ok(values[schema.index_of_column(column)?].clone()))
                        .collect::<Result<_>>()?;
                    schema = &alias.schema;
                }
                RowStep::Filter(predicate) => {
                    filter = filter.and(strip_outer_reference(inline_union_columns(
                        predicate.clone(),
                        schema,
                        &values,
                    )?));
                }
                RowStep::UnionOutput(expr, output_schema) => {
                    values = expr
                        .into_iter()
                        .map(|expr| inline_union_columns(expr, schema, &values))
                        .collect::<Result<_>>()?;
                    schema = output_schema;
                }
                _ => return Ok(None),
            }
        }
        let is_branch = Expr::Column(tag.clone()).eq(lit(index as u64));
        filters.push(is_branch.clone().and(filter));
        let branch_output = union.inputs[index].schema();
        if branch_output.fields().len() != output.len() {
            return internal_err!("correlated UNION inputs must have the same number of columns");
        }
        for (output, column) in output.iter_mut().zip(branch_output.columns()) {
            let value = values[schema.index_of_column(&column)?].clone();
            *output = when(is_branch.clone(), strip_outer_reference(value))
                .otherwise(std::mem::take(output))?;
        }
        let padded = sources
            .iter()
            .enumerate()
            .map(|(position, (name, data_type))| {
                let value = if (offset..offset + columns.len()).contains(&position) {
                    Expr::Column(columns[position - offset].clone())
                } else {
                    lit(ScalarValue::try_new_null(data_type)?)
                };
                Ok(value.alias(&name.name))
            })
            .collect::<Result<Vec<_>>>()?;
        inputs.push(Arc::new(
            LogicalPlanBuilder::from(branch.input)
                .project(
                    padded
                        .into_iter()
                        .chain([lit(index as u64).alias(&tag.name)]),
                )?
                .build()?,
        ));
        offset += columns.len();
    }
    steps.push(RowStep::UnionOutput(output, &union.schema));
    Ok(Some(CorrelatedRows {
        input: LogicalPlan::Union(Union::try_new(inputs)?),
        join_filter: disjunction(filters).unwrap_or_else(|| lit(false)),
        steps,
    }))
}

fn inline_union_columns(expr: Expr, schema: &DFSchema, values: &[Expr]) -> Result<Expr> {
    expr.transform_up(|expr| {
        if let Expr::Column(column) = &expr
            && let Ok(index) = schema.index_of_column(column)
        {
            return Ok(Transformed::yes(values[index].clone()));
        }
        Ok(Transformed::no(expr))
    })
    .data()
}

pub(super) fn move_correlated_inner_join_predicates(plan: LogicalPlan) -> Result<LogicalPlan> {
    plan.transform_up(|node| {
        let LogicalPlan::Join(mut join) = node else {
            return Ok(Transformed::no(node));
        };
        let Some(predicate) = join.filter.as_ref() else {
            return Ok(Transformed::no(LogicalPlan::Join(join)));
        };
        if join.join_type != JoinType::Inner || !predicate.contains_outer() {
            return Ok(Transformed::no(LogicalPlan::Join(join)));
        }
        let (correlated, local): (Vec<_>, Vec<_>) = split_conjunction(predicate)
            .into_iter()
            .cloned()
            .partition(Expr::contains_outer);
        let Some(predicate) = conjunction(correlated) else {
            return Ok(Transformed::no(LogicalPlan::Join(join)));
        };
        // For an inner join the correlated ON terms can filter the joined rows.
        join.filter = conjunction(local);
        Ok(Transformed::yes(LogicalPlan::Filter(Filter::try_new(
            predicate,
            Arc::new(LogicalPlan::Join(join)),
        )?)))
    })
    .data()
}

fn can_pull_up(plan: &LogicalPlan) -> Result<bool> {
    let mut supported = true;
    plan.apply(|node| {
        Ok(match node {
            LogicalPlan::Filter(_) | LogicalPlan::SubqueryAlias(_) => TreeNodeRecursion::Continue,
            LogicalPlan::Projection(_) if !node.contains_outer_reference() => {
                TreeNodeRecursion::Continue
            }
            // Uncorrelated subtrees keep their original operators, including joins.
            node if node.all_out_ref_exprs().is_empty() => TreeNodeRecursion::Jump,
            _ => {
                supported = false;
                TreeNodeRecursion::Stop
            }
        })
    })?;
    Ok(supported)
}

fn split_limit<'a>(limit: &'a Limit, steps: &mut Vec<RowStep<'a>>) -> &'a LogicalPlan {
    let mut input = limit.input.as_ref();
    let mut wrappers = Vec::new();
    while let LogicalPlan::Projection(projection) = input {
        // A projection can hide an ORDER BY key from the scalar result.
        wrappers.push(RowStep::Project(projection));
        input = &projection.input;
    }
    let sort = match input {
        LogicalPlan::Sort(sort) => Some(sort.clone()),
        LogicalPlan::Extension(extension) => extension
            .node
            .as_any()
            .downcast_ref::<RequiredSortNode>()
            .map(|sort| Sort {
                expr: sort.sort_expr().to_vec(),
                input: Arc::new(extension.node.inputs()[0].clone()),
                fetch: sort.fetch(),
            }),
        _ => None,
    };
    if let Some(sort) = sort {
        steps.extend(wrappers);
        steps.push(RowStep::Limit(limit, Some(sort)));
        input.inputs()[0]
    } else {
        steps.push(RowStep::Limit(limit, None));
        &limit.input
    }
}

fn is_correlated_self_equality(expr: &Expr) -> bool {
    matches!(expr, Expr::BinaryExpr(binary)
        if binary.op == Operator::Eq && binary.left == binary.right && expr.contains_outer())
}

pub(super) fn has_correlated_self_equality(plan: &LogicalPlan) -> Result<bool> {
    plan.exists(|node| {
        Ok(matches!(node, LogicalPlan::Filter(filter)
            if split_conjunction(&filter.predicate).into_iter().any(is_correlated_self_equality)))
    })
}

fn preserve_self_equalities(input: LogicalPlan) -> Result<LogicalPlan> {
    input
        .transform_up(|node| {
            let LogicalPlan::Filter(mut filter) = node else {
                return Ok(Transformed::no(node));
            };
            let mut changed = false;
            let predicates = split_conjunction(&filter.predicate)
                .into_iter()
                .map(|predicate| {
                    if is_correlated_self_equality(predicate) {
                        // Native pull-up discards bare self-equality even on NULL.
                        changed = true;
                        Expr::IsTrue(Box::new(predicate.clone()))
                    } else {
                        predicate.clone()
                    }
                })
                .collect::<Vec<_>>();
            if changed && let Some(predicate) = conjunction(predicates) {
                filter.predicate = predicate;
            }
            Ok(Transformed::new_transformed(
                LogicalPlan::Filter(filter),
                changed,
            ))
        })
        .data()
}

pub(super) fn apply_row_steps(
    mut plan: LogicalPlan,
    steps: Vec<RowStep<'_>>,
    row_id: &Column,
    marker: &Column,
    outer_columns: &[Column],
) -> Result<LogicalPlan> {
    for step in steps.into_iter().rev() {
        plan = match step {
            RowStep::Project(project) => project_inner_rows(
                plan,
                project.expr.iter().cloned(),
                &project.schema,
                row_id,
                marker,
                outer_columns,
            )?,
            RowStep::Alias(alias) => project_inner_rows(
                plan,
                alias.input.schema().columns().into_iter().map(Expr::Column),
                &alias.schema,
                row_id,
                marker,
                outer_columns,
            )?,
            RowStep::Filter(predicate) => clear_match_marker_on_rejection(
                plan,
                marker,
                strip_outer_reference(predicate.clone()),
            )?,
            RowStep::Aggregate(aggregate) => {
                aggregate_inner_rows(plan, aggregate, row_id, marker, outer_columns)?
            }
            RowStep::Distinct(input) => {
                distinct_inner_rows(plan, input, row_id, marker, outer_columns)?
            }
            RowStep::Limit(limit, sort) => {
                limit_inner_rows(plan, limit, sort.as_ref(), row_id, marker)?
            }
            RowStep::Window(window) => window_inner_rows(plan, window, row_id, marker)?,
            RowStep::Sort(sort) => {
                let mut sort = sort.clone();
                for key in &mut sort.expr {
                    key.expr = guard_unmatched(
                        strip_outer_reference(std::mem::take(&mut key.expr)),
                        &Expr::Column(marker.clone()).is_not_null(),
                    )?;
                }
                sort.input = Arc::new(plan);
                LogicalPlan::Sort(sort)
            }
            RowStep::WindowOrder(extension) => {
                let expressions = extension
                    .node
                    .expressions()
                    .into_iter()
                    .map(|expr| {
                        guard_unmatched(
                            strip_outer_reference(expr),
                            &Expr::Column(marker.clone()).is_not_null(),
                        )
                    })
                    .collect::<Result<_>>()?;
                LogicalPlan::Extension(Extension {
                    node: extension
                        .node
                        .with_exprs_and_inputs(expressions, vec![plan])?,
                })
            }
            RowStep::UnionOutput(expr, schema) => {
                project_inner_rows(plan, expr, schema, row_id, marker, outer_columns)?
            }
        };
    }
    Ok(plan)
}

fn window_inner_rows(
    plan: LogicalPlan,
    window: &Window,
    row_id: &Column,
    marker: &Column,
) -> Result<LogicalPlan> {
    let is_match = Expr::Column(marker.clone()).is_not_null();
    let names = NamePreserver::new_for_projection();
    let expressions = window
        .window_expr
        .iter()
        .map(|expr| {
            let name = names.save(expr);
            let expr = strip_outer_reference(expr.clone())
                .transform_up(|expr| {
                    let Expr::WindowFunction(mut function) = expr else {
                        return Ok(Transformed::no(expr));
                    };
                    for (index, arg) in function.params.args.iter_mut().enumerate() {
                        // These scalar parameters must remain constant for the window evaluator.
                        if let WindowFunctionDefinition::WindowUDF(udwf) = &function.fun
                            && matches!(
                                (udwf.name(), index),
                                ("lag" | "lead" | "nth_value", 1) | ("ntile", 0)
                            )
                        {
                            continue;
                        }
                        *arg = guard_unmatched(std::mem::take(arg), &is_match)?;
                    }
                    for partition in &mut function.params.partition_by {
                        *partition = guard_unmatched(std::mem::take(partition), &is_match)?;
                    }
                    for sort in &mut function.params.order_by {
                        sort.expr = guard_unmatched(std::mem::take(&mut sort.expr), &is_match)?;
                    }
                    if let Some(filter) = function.params.filter.take() {
                        function.params.filter = Some(Box::new(
                            when(is_match.clone(), *filter).otherwise(lit(false))?,
                        ));
                    }
                    // Rejected rows preserve the outer group but cannot affect a live frame.
                    function
                        .params
                        .partition_by
                        .splice(0..0, [Expr::Column(row_id.clone()), is_match.clone()]);
                    Ok(Transformed::yes(Expr::WindowFunction(function)))
                })?
                .data;
            Ok(name.restore(expr))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(LogicalPlan::Window(Window::try_new(
        expressions,
        Arc::new(plan),
    )?))
}

fn project_inner_rows(
    plan: LogicalPlan,
    inner: impl IntoIterator<Item = Expr>,
    output: &DFSchema,
    row_id: &Column,
    marker: &Column,
    outer_columns: &[Column],
) -> Result<LogicalPlan> {
    let is_match = Expr::Column(marker.clone()).is_not_null();
    let mut expressions = Vec::new();
    for (expr, column) in inner.into_iter().zip(output.columns()) {
        let expr = strip_outer_reference(expr.unalias());
        let expr = match expr {
            Expr::Column(_) | Expr::Literal(_, _) => expr,
            expr => when(is_match.clone(), expr).end()?,
        };
        expressions.push(expr.alias_qualified(column.relation, column.name));
    }
    expressions.extend(
        outer_columns
            .iter()
            .chain([row_id, marker])
            .cloned()
            .map(Expr::Column),
    );
    LogicalPlanBuilder::from(plan).project(expressions)?.build()
}
