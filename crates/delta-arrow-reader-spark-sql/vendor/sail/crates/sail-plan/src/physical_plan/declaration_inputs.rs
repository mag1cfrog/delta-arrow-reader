// Local Spark unevaluated-input rewrite. See the host crate's UPSTREAM.md.
//! A Spark expression can keep an argument it never evaluates, such as the format of
//! UNIX_TIMESTAMP over a DATE. DAR's nullability declarations, `SparkKnownNullable`,
//! `SparkKnownNotNull` and `SparkWithInputs`, keep that argument's columns, aggregates and window
//! functions as unused arguments after the value they declare, so that the analyzer checks them
//! as Spark's does, the operators below compute the aggregates and window functions, and constant
//! folding leaves the declaration as Spark leaves its expression. Once the physical plan is
//! optimized, folding is over and every output field is fixed, so the inputs have done their
//! work; but DataFusion's projection pushdown can inline a column's expression into them, or keep
//! a projection below a union that computes them, and evaluating them could fail where Spark's
//! expression does not. This rewrite drops the inputs and prunes what only they read: it puts a
//! projection of the columns still read under the declaring projection, or under each input of a
//! union there, which DataFusion's projection pushdown then merges with the projections below.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use datafusion::physical_expr::ScalarFunctionExpr;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::utils::collect_columns;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::projection_pushdown::ProjectionPushdown;
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode};

/// The functions that keep their arguments after the first as unused inputs.
const DECLARATIONS: [&str; 3] = [
    "spark_known_nullable",
    "spark_known_not_null",
    "spark_with_inputs",
];

/// `plan` with the inputs of every declaration in its projections dropped, and the columns that
/// only those inputs read pruned below.
pub(super) fn drop_declaration_inputs(
    plan: Arc<dyn ExecutionPlan>,
    config: &ConfigOptions,
) -> Result<Arc<dyn ExecutionPlan>> {
    let plan = plan.transform_up(|node| {
        let Some(projection) = node.downcast_ref::<ProjectionExec>() else {
            return Ok(Transformed::no(node));
        };
        let mut transformed = false;
        let mut exprs = projection
            .expr()
            .iter()
            .map(|ProjectionExpr { expr, alias }| {
                let expr = without_inputs(Arc::clone(expr))?;
                transformed |= expr.transformed;
                Ok(ProjectionExpr {
                    expr: expr.data,
                    alias: alias.clone(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        if !transformed {
            return Ok(Transformed::no(node));
        }
        let input = Arc::clone(projection.input());
        let schema = input.schema();
        let read = exprs
            .iter()
            .flat_map(|ProjectionExpr { expr, .. }| collect_columns(expr))
            .map(|column| column.index())
            .collect::<BTreeSet<_>>();
        let input = if read.len() < schema.fields().len() {
            let positions = read
                .iter()
                .enumerate()
                .map(|(position, index)| (*index, position))
                .collect::<HashMap<_, _>>();
            exprs = exprs
                .into_iter()
                .map(|ProjectionExpr { expr, alias }| {
                    let expr = expr
                        .transform_up(|expr| {
                            let Some(column) = expr.downcast_ref::<Column>() else {
                                return Ok(Transformed::no(expr));
                            };
                            let position = positions[&column.index()];
                            Ok(Transformed::yes(
                                Arc::new(Column::new(column.name(), position))
                                    as Arc<dyn PhysicalExpr>,
                            ))
                        })?
                        .data;
                    Ok(ProjectionExpr { expr, alias })
                })
                .collect::<Result<Vec<_>>>()?;
            match input.downcast_ref::<UnionExec>() {
                Some(union) => UnionExec::try_new(
                    union
                        .inputs()
                        .iter()
                        .map(|input| columns_of(Arc::clone(input), &read))
                        .collect::<Result<Vec<_>>>()?,
                )?,
                None => columns_of(input, &read)?,
            }
        } else {
            input
        };
        let projection = ProjectionExec::try_new(exprs, input)?;
        Ok(Transformed::yes(
            Arc::new(projection) as Arc<dyn ExecutionPlan>
        ))
    })?;
    if plan.transformed {
        ProjectionPushdown::new().optimize(plan.data, config)
    } else {
        Ok(plan.data)
    }
}

/// A projection of `input`'s columns at the positions `read`.
fn columns_of(
    input: Arc<dyn ExecutionPlan>,
    read: &BTreeSet<usize>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let schema = input.schema();
    let columns = read.iter().map(|index| {
        let name = schema.field(*index).name();
        ProjectionExpr {
            expr: Arc::new(Column::new(name, *index)) as Arc<dyn PhysicalExpr>,
            alias: name.clone(),
        }
    });
    Ok(Arc::new(ProjectionExec::try_new(columns, input)?))
}

/// `expr` with each declaration reduced to the value it declares, keeping its declared field.
fn without_inputs(expr: Arc<dyn PhysicalExpr>) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
    expr.transform_up(|expr| {
        let Some(function) = expr.downcast_ref::<ScalarFunctionExpr>() else {
            return Ok(Transformed::no(expr));
        };
        let [value, _, ..] = function.args() else {
            return Ok(Transformed::no(expr));
        };
        if !DECLARATIONS.contains(&function.fun().name()) {
            return Ok(Transformed::no(expr));
        }
        let value = Arc::clone(value);
        Ok(Transformed::yes(expr.with_new_children(vec![value])?))
    })
}
