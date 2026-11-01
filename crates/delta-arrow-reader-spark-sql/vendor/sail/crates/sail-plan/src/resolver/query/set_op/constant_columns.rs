//! Track constants whose negative zero survives DISTINCT set operations.
//! A literal result can remain `-0.0`, while the same value read from VALUES or
//! a table becomes `0.0`. An outer join hides that constant when it can add NULL;
//! a surrounding INTERSECT can rule those NULL rows out again.

use std::sync::{Arc, LazyLock};

use arrow::datatypes::{DataType, FieldRef};
use datafusion::execution::SessionState;
use datafusion::optimizer::OptimizerRule;
use datafusion::optimizer::optimize_projections::OptimizeProjections;
use datafusion::optimizer::simplify_expressions::{ExprSimplifier, SimplifyExpressions};
use datafusion::optimizer::utils::NamePreserver;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::utils::take_function_args;
use datafusion_common::{DFSchema, DFSchemaRef, JoinType, Result};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    ColumnarValue, Distinct, Expr, ExprSchemable, LogicalPlan, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDFImpl, Signature, Volatility,
};
use sail_logical_plan::sort::SortWithinPartitionsNode;

pub(super) fn build_set_result_constant_markers(
    analyzed: &LogicalPlan,
    session: &SessionState,
) -> Result<Vec<Option<PreserveSetConstant>>> {
    Ok(constant_output_columns(analyzed)?
        .into_iter()
        .zip(prove_non_nullable_outputs(analyzed, session)?)
        .map(|(constant, non_nullable)| {
            constant.then_some(PreserveSetConstant {
                nullable: !non_nullable,
            })
        })
        .collect())
}

pub(super) fn prove_non_nullable_outputs(
    plan: &LogicalPlan,
    session: &SessionState,
) -> Result<Vec<bool>> {
    let simplified = make_query_time_values_opaque(plan.clone())
        .and_then(|opaque| {
            OptimizeProjections::new()
                .rewrite(opaque, session)
                .map(|plan| plan.data)
        })
        .and_then(|opaque| {
            opaque
                .transform_up_with_subqueries(|plan| {
                    let names = NamePreserver::new(&plan);
                    let inputs = plan.inputs();
                    let substituted = plan
                        .clone()
                        .map_expressions(|expr| {
                            let name = names.save(&expr);
                            expr.transform_up(|expr| {
                                if let Expr::Column(column) = &expr {
                                    for input in &inputs {
                                        if let Ok(index) = input.schema().index_of_column(column)
                                            && let Some(literal) = projected_literal(input, index)
                                        {
                                            return Ok(Transformed::yes(literal));
                                        }
                                    }
                                }
                                Ok(Transformed::no(expr))
                            })
                            .map(|result| result.update_data(|expr| name.restore(expr)))
                        })?
                        .data;
                    SimplifyExpressions::new()
                        .rewrite(substituted, session)?
                        .data
                        .recompute_schema()
                        .map(Transformed::yes)
                })
                .map(|plan| plan.data)
        })
        .unwrap_or_else(|_| plan.clone());
    Ok(plan
        .schema()
        .fields()
        .iter()
        .zip(simplified.schema().fields())
        // Folding can remove a marker and conservatively widen its proven nonnull field.
        .map(|(original, folded)| !original.is_nullable() || !folded.is_nullable())
        .collect())
}

// Carry only folded literals across aliases; copying computed expressions expands diamonds.
fn projected_literal(plan: &LogicalPlan, index: usize) -> Option<Expr> {
    match plan {
        LogicalPlan::Projection(projection) => {
            let mut expr = projection.expr.get(index)?;
            while let Expr::Alias(alias) = expr {
                expr = &alias.expr;
            }
            match expr {
                Expr::Literal(..) => Some(expr.clone()),
                Expr::Column(column) => projected_literal(
                    &projection.input,
                    projection.input.schema().index_of_column(column).ok()?,
                ),
                _ => None,
            }
        }
        LogicalPlan::SubqueryAlias(alias) => projected_literal(&alias.input, index),
        LogicalPlan::Filter(filter) => projected_literal(&filter.input, index),
        LogicalPlan::Sort(sort) => projected_literal(&sort.input, index),
        LogicalPlan::Limit(limit) => projected_literal(&limit.input, index),
        LogicalPlan::Repartition(repartition) => projected_literal(&repartition.input, index),
        _ => None,
    }
}

// Typed variables block native evaluation without blocking immutable sibling expressions.
fn make_query_time_values_opaque(plan: LogicalPlan) -> Result<LogicalPlan> {
    let mut next_variable = 0_usize;
    plan.transform_up_with_subqueries(|plan| {
        let schema = match &plan {
            LogicalPlan::TableScan(scan) => {
                DFSchema::try_from_qualified_schema(scan.table_name.clone(), &scan.source.schema())?
            }
            _ => datafusion_expr::utils::merge_schema(&plan.inputs()),
        };
        let names = NamePreserver::new(&plan);
        plan.map_expressions(|expr| {
            let name = names.save(&expr);
            make_query_time_expression_opaque(expr, &schema, &mut next_variable)
                .map(|expr| Transformed::yes(name.restore(expr)))
        })
    })
    .map(|plan| plan.data)
}

pub(super) fn fold_immutable_expression(
    expr: Expr,
    schema: DFSchemaRef,
    session: &SessionState,
) -> Result<Expr> {
    let expr = make_query_time_expression_opaque(expr, &schema, &mut 0)?;
    let simplifier = ExprSimplifier::new(
        SimplifyContext::builder()
            .with_schema(schema.clone())
            .with_config_options(session.config_options().clone())
            .build(),
    );
    simplifier.simplify(simplifier.coerce(expr, &schema)?)
}

fn make_query_time_expression_opaque(
    expr: Expr,
    schema: &DFSchema,
    next_variable: &mut usize,
) -> Result<Expr> {
    expr.transform_down(|expr| {
        let query_dependent = match &expr {
            Expr::ScalarFunction(function) => {
                function.func.signature().volatility != Volatility::Immutable
            }
            Expr::HigherOrderFunction(function) => {
                function.func.signature().volatility != Volatility::Immutable
            }
            Expr::ScalarVariable(..) => true,
            _ => false,
        };
        if !query_dependent {
            return Ok(Transformed::no(expr));
        }
        let (_, field) = expr.to_field(schema)?;
        *next_variable += 1;
        Ok(Transformed::yes(Expr::ScalarVariable(
            field,
            vec!["__dar_set_query_value".into(), next_variable.to_string()],
        )))
    })
    .map(|result| result.data)
}

pub(super) fn constant_output_columns(plan: &LogicalPlan) -> Result<Vec<bool>> {
    Ok(analyze_constant_outputs(plan)?.constant_columns)
}

struct ConstantOutputFacts {
    constant_columns: Vec<bool>,
    materialized_inline_rows: bool,
}

fn analyze_constant_outputs(plan: &LogicalPlan) -> Result<ConstantOutputFacts> {
    let mut materialized_inline_rows = false;
    let columns = match plan {
        LogicalPlan::Projection(projection) => {
            let input = analyze_constant_outputs(&projection.input)?;
            materialized_inline_rows = input.materialized_inline_rows;
            for expr in &projection.expr {
                materialized_inline_rows &= can_evaluate_inline_expression(expr)?;
            }
            if materialized_inline_rows {
                vec![false; projection.expr.len()]
            } else {
                projection
                    .expr
                    .iter()
                    .map(|expr| {
                        expression_uses_only_constants(
                            expr,
                            projection.input.schema(),
                            &input.constant_columns,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?
            }
        }
        LogicalPlan::Aggregate(aggregate)
            if !aggregate
                .group_expr
                .iter()
                .any(|expr| matches!(expr, Expr::GroupingSet(_))) =>
        {
            let constants = constant_output_columns(&aggregate.input)?;
            let mut result = aggregate
                .group_expr
                .iter()
                .map(|expr| {
                    expression_uses_only_constants(expr, aggregate.input.schema(), &constants)
                })
                .collect::<Result<Vec<_>>>()?;
            result.resize(plan.schema().fields().len(), false);
            result
        }
        LogicalPlan::SubqueryAlias(alias) => return analyze_constant_outputs(&alias.input),
        LogicalPlan::Filter(filter) => {
            let mut input = analyze_constant_outputs(&filter.input)?;
            input.materialized_inline_rows &= can_evaluate_inline_expression(&filter.predicate)?;
            return Ok(input);
        }
        LogicalPlan::Limit(limit) => return analyze_constant_outputs(&limit.input),
        LogicalPlan::Sort(sort) => constant_output_columns(&sort.input)?,
        LogicalPlan::Extension(extension) => match extension
            .node
            .as_any()
            .downcast_ref::<SortWithinPartitionsNode>()
        {
            Some(sort) => constant_output_columns(sort.input())?,
            None => vec![false; plan.schema().fields().len()],
        },
        LogicalPlan::Repartition(repartition) => constant_output_columns(&repartition.input)?,
        LogicalPlan::Distinct(Distinct::All(input)) => constant_output_columns(input)?,
        LogicalPlan::Window(window) => {
            let mut constants = constant_output_columns(&window.input)?;
            constants.resize(plan.schema().fields().len(), false);
            constants
        }
        LogicalPlan::Join(join) => {
            let mut left = vec![false; join.left.schema().fields().len()];
            let mut right = vec![false; join.right.schema().fields().len()];
            if matches!(
                join.join_type,
                JoinType::Inner
                    | JoinType::Left
                    | JoinType::LeftSemi
                    | JoinType::LeftAnti
                    | JoinType::LeftMark
            ) {
                left = constant_output_columns(&join.left)?;
            }
            if matches!(
                join.join_type,
                JoinType::Inner
                    | JoinType::Right
                    | JoinType::RightSemi
                    | JoinType::RightAnti
                    | JoinType::RightMark
            ) {
                right = constant_output_columns(&join.right)?;
            }
            match join.join_type {
                JoinType::LeftSemi | JoinType::LeftAnti => left,
                JoinType::RightSemi | JoinType::RightAnti => right,
                JoinType::LeftMark => {
                    left.push(false);
                    left
                }
                JoinType::RightMark => {
                    right.push(false);
                    right
                }
                _ => {
                    left.extend(right);
                    left
                }
            }
        }
        LogicalPlan::Values(values) => {
            materialized_inline_rows = true;
            for expr in values.values.iter().flatten() {
                materialized_inline_rows &= can_evaluate_inline_expression(expr)?;
            }
            vec![false; plan.schema().fields().len()]
        }
        // Aggregate values and set bags depend on rows.
        _ => vec![false; plan.schema().fields().len()],
    };
    Ok(ConstantOutputFacts {
        constant_columns: columns,
        materialized_inline_rows,
    })
}

// Spark materializes eligible VALUES projections before set planning, losing literal lineage.
fn can_evaluate_inline_expression(expr: &Expr) -> Result<bool> {
    Ok(!expr.exists(|expr| {
        Ok(matches!(
            expr,
            Expr::OuterReferenceColumn(..)
                | Expr::ScalarSubquery(_)
                | Expr::Exists(_)
                | Expr::InSubquery(_)
                | Expr::AggregateFunction(_)
                | Expr::WindowFunction(_)
                | Expr::Placeholder(_)
                | Expr::GroupingSet(_)
                | Expr::Unnest(_)
                | Expr::ScalarVariable(..)
        ))
    })?)
}

pub(super) fn expression_uses_only_constants(
    expr: &Expr,
    schema: &DFSchema,
    columns: &[bool],
) -> Result<bool> {
    let mut constant = true;
    expr.apply(|expr| {
        if matches!(expr, Expr::ScalarFunction(function)
            if function.func.inner().is::<PreserveSetConstant>())
        {
            return Ok(TreeNodeRecursion::Jump);
        }
        constant = match expr {
            Expr::Column(column) => columns[schema.index_of_column(column)?],
            Expr::OuterReferenceColumn(..)
            | Expr::ScalarSubquery(_)
            | Expr::Exists(_)
            | Expr::InSubquery(_)
            | Expr::AggregateFunction(_)
            | Expr::WindowFunction(_)
            | Expr::Placeholder(_)
            | Expr::GroupingSet(_)
            | Expr::Unnest(_) => false,
            _ => !expr.is_volatile_node(),
        };
        Ok(if constant {
            TreeNodeRecursion::Continue
        } else {
            TreeNodeRecursion::Stop
        })
    })?;
    Ok(constant)
}

// Keep constant provenance through set joins until resolution ends, without copying expressions.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct PreserveSetConstant {
    pub(super) nullable: bool,
}

impl ScalarUDFImpl for PreserveSetConstant {
    fn name(&self) -> &str {
        "preserve_set_constant"
    }

    fn signature(&self) -> &Signature {
        static SIGNATURE: LazyLock<Signature> = LazyLock::new(|| {
            Signature::uniform(
                1,
                vec![DataType::Float32, DataType::Float64],
                Volatility::Immutable,
            )
        });
        &SIGNATURE
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        let [arg] = take_function_args(self.name(), args)?;
        Ok(arg.clone())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [arg] = take_function_args(self.name(), args.arg_fields)?;
        Ok(Arc::new(arg.as_ref().clone().with_nullable(self.nullable)))
    }

    fn simplify(&self, args: Vec<Expr>, _: &SimplifyContext) -> Result<ExprSimplifyResult> {
        let [arg] = take_function_args(self.name(), args)?;
        Ok(ExprSimplifyResult::Simplified(arg))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [arg] = take_function_args(self.name(), args.args)?;
        Ok(arg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_common::{Column, NullEquality};
    use datafusion_expr::{Join, JoinConstraint, LogicalPlanBuilder, col, lit};
    use std::sync::Arc;

    #[test]
    fn join_constants_follow_only_the_preserved_inputs() -> Result<()> {
        let left = LogicalPlanBuilder::empty(true)
            .project([lit(-0.0_f64).alias("left")])?
            .build()?;
        let right = LogicalPlanBuilder::empty(true)
            .project([lit(-0.0_f64).alias("right")])?
            .build()?;
        for (kind, expected) in [
            (JoinType::Inner, vec![true, true]),
            (JoinType::Left, vec![true, false]),
            (JoinType::Right, vec![false, true]),
            (JoinType::Full, vec![false, false]),
            (JoinType::LeftSemi, vec![true]),
            (JoinType::LeftAnti, vec![true]),
            (JoinType::RightSemi, vec![true]),
            (JoinType::RightAnti, vec![true]),
            (JoinType::LeftMark, vec![true, false]),
            (JoinType::RightMark, vec![true, false]),
        ] {
            let join = Join::try_new(
                Arc::new(left.clone()),
                Arc::new(right.clone()),
                vec![(
                    Expr::Column(Column::from_name("left")),
                    Expr::Column(Column::from_name("right")),
                )],
                None,
                kind,
                JoinConstraint::On,
                NullEquality::NullEqualsNull,
                false,
            )?;
            assert_eq!(
                constant_output_columns(&LogicalPlan::Join(join))?,
                expected,
                "{kind}"
            );
        }
        Ok(())
    }

    #[test]
    fn shared_projection_inputs_do_not_expand_constant_expressions() -> Result<()> {
        let mut plan = LogicalPlanBuilder::empty(true)
            .project([lit(0.0_f64).alias("a"), lit(-0.0_f64).alias("b")])?
            .build()?;
        for _ in 0..40 {
            plan = LogicalPlanBuilder::from(plan)
                .project([
                    (col("a") + col("b")).alias("a"),
                    (col("a") - col("b")).alias("b"),
                ])?
                .build()?;
        }
        assert_eq!(constant_output_columns(&plan)?, vec![true, true]);
        Ok(())
    }
}
