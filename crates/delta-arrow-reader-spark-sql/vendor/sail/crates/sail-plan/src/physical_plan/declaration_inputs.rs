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
//! the inputs from every operator's expressions, except the outputs of aggregates and window
//! functions, which Spark computes, and prunes the columns that only the inputs read.

use std::collections::HashSet;

use datafusion::optimizer::optimize_projections::OptimizeProjections;
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{Column, Result};
use datafusion_expr::expr_rewriter::NamePreserver;
use datafusion_expr::{Expr, LogicalPlan};

/// The functions that keep their arguments after the first as unused inputs.
const DECLARATIONS: [&str; 3] = [
    "spark_known_nullable",
    "spark_known_not_null",
    "spark_with_inputs",
];

/// `plan` with the inputs of every declaration dropped, except the outputs of aggregates and
/// window functions, and the columns that only those inputs read pruned.
pub(super) fn drop_declaration_inputs(
    plan: LogicalPlan,
    config: &dyn OptimizerConfig,
) -> Result<LogicalPlan> {
    let computed = computed_columns(&plan)?;
    let plan = plan.transform_up_with_subqueries(|node| {
        // A window or aggregate expression's name is its schema field, which must not change.
        let names = NamePreserver::new(&node);
        node.map_expressions(|expr| {
            let name = names.save(&expr);
            let expr = expr.transform_up(|expr| {
                let Expr::ScalarFunction(function) = &expr else {
                    return Ok(Transformed::no(expr));
                };
                if function.args.len() < 2 || !DECLARATIONS.contains(&function.name()) {
                    return Ok(Transformed::no(expr));
                }
                let Expr::ScalarFunction(mut function) = expr else {
                    unreachable!("matched a scalar function above")
                };
                let inputs = function.args.split_off(1);
                let dropped = inputs.len();
                function.args.extend(inputs.into_iter().filter(|input| {
                    matches!(input, Expr::Column(column) if computed.contains(column.name()))
                }));
                let transformed = function.args.len() - 1 < dropped;
                Ok(Transformed::new_transformed(
                    Expr::ScalarFunction(function),
                    transformed,
                ))
            })?;
            Ok(expr.update_data(|expr| name.restore(expr)))
        })
    })?;
    if plan.transformed {
        Ok(OptimizeProjections::new().rewrite(plan.data, config)?.data)
    } else {
        Ok(plan.data)
    }
}

/// The column `expr` passes on, under any aliases.
fn column_of(expr: &Expr) -> Option<&Column> {
    match expr {
        Expr::Column(column) => Some(column),
        Expr::Alias(alias) => column_of(&alias.expr),
        _ => None,
    }
}

/// The names of the columns that aggregates and window functions compute in `plan`, also as
/// projections pass them on under other names.
fn computed_columns(plan: &LogicalPlan) -> Result<HashSet<String>> {
    let mut computed = HashSet::new();
    loop {
        let known = computed.len();
        plan.apply_with_subqueries(|node| {
            match node {
                LogicalPlan::Aggregate(aggregate) => computed.extend(
                    aggregate
                        .schema
                        .fields()
                        .iter()
                        .skip(aggregate.group_expr.len())
                        .map(|field| field.name().clone()),
                ),
                LogicalPlan::Window(window) => computed.extend(
                    window
                        .schema
                        .fields()
                        .iter()
                        .skip(window.input.schema().fields().len())
                        .map(|field| field.name().clone()),
                ),
                LogicalPlan::Projection(projection) => {
                    let passed = projection
                        .expr
                        .iter()
                        .zip(projection.schema.fields())
                        .filter(|(expr, _)| {
                            column_of(expr).is_some_and(|column| computed.contains(column.name()))
                        })
                        .map(|(_, field)| field.name().clone())
                        .collect::<Vec<_>>();
                    computed.extend(passed);
                }
                _ => {}
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        if computed.len() == known {
            return Ok(computed);
        }
    }
}
