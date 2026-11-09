// Local Spark unevaluated-input rewrite. See the host crate's UPSTREAM.md.
//! A Spark expression can keep an argument it never evaluates, such as the format of
//! UNIX_TIMESTAMP over a DATE. DAR's nullability declarations, `SparkKnownNullable`,
//! `SparkKnownNotNull` and `SparkWithInputs`, keep that argument's columns, aggregates and window
//! functions as unused arguments after the value they declare, so that the analyzer checks them
//! as Spark's does, the aggregates and window functions are computed, and constant folding leaves
//! the declaration as Spark leaves its expression. Once the logical plan is optimized, folding is
//! over and every output field is fixed, so the inputs have done their work, but evaluating them
//! could fail where Spark's expression does not: DataFusion's projection pushdown can inline a
//! column's expression into them, and any column they read is computed below. This rewrite drops
//! the inputs from every operator's expressions and prunes the columns that only they read.
//! Spark still computes the aggregates and window functions the argument reads, also through the
//! columns of a subquery, so a filter that always passes reads their outputs above their operator.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use datafusion::optimizer::optimize_projections::OptimizeProjections;
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, Result};
use datafusion_expr::expr_rewriter::NamePreserver;
use datafusion_expr::{Expr, Filter, LogicalPlan, ScalarUDF, lit};
use sail_function::scalar::spark_with_inputs::SparkWithInputs;

/// The functions that keep their arguments after the first as unused inputs.
const DECLARATIONS: [&str; 3] = [
    "spark_known_nullable",
    "spark_known_not_null",
    "spark_with_inputs",
];

/// `plan` with the inputs of every declaration dropped, the columns that only they read pruned,
/// and the outputs of aggregates and window functions they read kept computed.
pub(crate) fn drop_declaration_inputs(
    plan: LogicalPlan,
    config: &dyn OptimizerConfig,
) -> Result<LogicalPlan> {
    let mut inputs = vec![];
    // DataFusion has decorrelated most subqueries into joins by now; this also reaches the rest.
    plan.apply_with_subqueries(|node| {
        node.apply_expressions(|expr| {
            expr.apply(|expr| {
                if let Some(args) = declared_inputs(expr) {
                    inputs.extend(args.iter().cloned());
                }
                Ok(TreeNodeRecursion::Continue)
            })
        })
    })?;
    if inputs.is_empty() {
        return Ok(plan);
    }
    let read = computed_reads(&plan, &inputs)?;
    let plan = plan.transform_up_with_subqueries(|node| {
        // A window or aggregate expression's name is its schema field, which must not change.
        let names = NamePreserver::new(&node);
        node.map_expressions(|expr| {
            let name = names.save(&expr);
            let expr = expr.transform_up(|expr| {
                let Expr::ScalarFunction(mut function) = expr else {
                    return Ok(Transformed::no(expr));
                };
                if function.args.len() < 2 || !DECLARATIONS.contains(&function.name()) {
                    return Ok(Transformed::no(Expr::ScalarFunction(function)));
                }
                function.args.truncate(1);
                Ok(Transformed::yes(Expr::ScalarFunction(function)))
            })?;
            Ok(expr.update_data(|expr| name.restore(expr)))
        })?
        .transform_data(|node| keep_computed(node, &read))
    })?;
    Ok(OptimizeProjections::new().rewrite(plan.data, config)?.data)
}

/// The inputs `expr` keeps, if it is a declaration.
fn declared_inputs(expr: &Expr) -> Option<&[Expr]> {
    match expr {
        Expr::ScalarFunction(function) if DECLARATIONS.contains(&function.name()) => {
            function.args.get(1..)
        }
        _ => None,
    }
}

/// The columns `node` computes, if it is an aggregate or a window: those after its group keys,
/// grouping id included, or after its input's columns.
fn computed_columns(node: &LogicalPlan) -> Result<Vec<Column>> {
    let passed = match node {
        LogicalPlan::Aggregate(aggregate) => aggregate.group_expr_len()?,
        LogicalPlan::Window(window) => window.input.schema().fields().len(),
        _ => return Ok(vec![]),
    };
    Ok(node.schema().columns().into_iter().skip(passed).collect())
}

/// The names of the columns aggregates and window functions compute that `inputs` read, also
/// through the projections that define the columns they read. Sail names the columns of a union's
/// inputs like its own, so these are the projections of every input.
fn computed_reads(plan: &LogicalPlan, inputs: &[Expr]) -> Result<HashSet<String>> {
    let mut computed = HashSet::new();
    let mut definitions: HashMap<String, Vec<Expr>> = HashMap::new();
    plan.apply_with_subqueries(|node| {
        computed.extend(
            computed_columns(node)?
                .into_iter()
                .map(|column| column.name),
        );
        if let LogicalPlan::Projection(projection) = node {
            for (expr, field) in projection.expr.iter().zip(projection.schema.fields()) {
                let definition = definitions.entry(field.name().clone()).or_default();
                definition.push(expr.clone());
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    let mut read = HashSet::new();
    let mut seen = HashSet::new();
    let mut pending = inputs
        .iter()
        .flat_map(|input| input.column_refs())
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    while let Some(name) = pending.pop() {
        if !seen.insert(name.clone()) {
            continue;
        }
        if computed.contains(&name) {
            read.insert(name);
        } else if let Some(definition) = definitions.get(&name) {
            for expr in definition {
                pending.extend(
                    expr.column_refs()
                        .into_iter()
                        .map(|column| column.name.clone()),
                );
            }
        }
    }
    Ok(read)
}

/// `node` under a filter that always passes and reads the outputs in `read` it computes, so that
/// pruning keeps them, if it computes any.
fn keep_computed(node: LogicalPlan, read: &HashSet<String>) -> Result<Transformed<LogicalPlan>> {
    let kept = computed_columns(&node)?
        .into_iter()
        .filter(|column| read.contains(&column.name))
        .map(Expr::Column)
        .collect::<Vec<_>>();
    if kept.is_empty() {
        return Ok(Transformed::no(node));
    }
    let predicate = ScalarUDF::from(SparkWithInputs::new()).call([vec![lit(true)], kept].concat());
    Ok(Transformed::yes(LogicalPlan::Filter(Filter::try_new(
        predicate,
        Arc::new(node),
    )?)))
}
