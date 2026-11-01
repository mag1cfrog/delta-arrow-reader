//! Keep INTERSECT/EXCEPT visible until their enclosing query has been resolved.
//! A filter above an inner set can change whether Spark retains a literal -0.0.

use std::fmt::Formatter;
use std::sync::Arc;

use arrow::datatypes::DataType;
use datafusion::execution::SessionState;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{DFSchema, DFSchemaRef, JoinType, Result, internal_err};
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};

use super::constant_columns::{
    build_set_result_constant_markers, constant_output_columns, expression_uses_only_constants,
    prove_non_nullable_outputs,
};
use super::filter_context::EnclosingQueryContext;
use super::lower_set_operation;
use crate::function::common::SparkComparisonFloat;
use crate::resolver::state::PlanResolverState;

pub(super) fn lower_query_sets(
    plan: &LogicalPlan,
    mut session: SessionState,
    state: &mut PlanResolverState,
) -> Result<LogicalPlan> {
    let mut has_sets = false;
    plan.apply_with_subqueries(|plan| {
        if matches!(plan, LogicalPlan::Extension(extension)
            if extension.node.as_any().is::<PendingSetOperation>())
        {
            has_sets = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    if !has_sets {
        return Ok(plan.clone());
    }
    session.execution_props_mut().query_execution_start_time = None;
    Ok(lower_sets(plan, &mut EnclosingQueryContext::default(), &session, state)?.executable)
}

struct SetLoweringResult {
    executable: LogicalPlan,
    constant_analysis: LogicalPlan,
}

fn lower_sets<'a>(
    plan: &'a LogicalPlan,
    context: &mut EnclosingQueryContext<'a>,
    session: &SessionState,
    state: &mut PlanResolverState,
) -> Result<SetLoweringResult> {
    if let LogicalPlan::Extension(extension) = plan
        && let Some(set) = extension
            .node
            .as_any()
            .downcast_ref::<PendingSetOperation>()
    {
        let right = lower_sets(
            &set.right,
            &mut EnclosingQueryContext::default(),
            session,
            state,
        )?;
        let saved = context.enter_input(plan, 0);
        if !set.is_all && set.join_type == JoinType::LeftSemi {
            for (index, (constant, nonnull)) in constant_output_columns(&right.constant_analysis)?
                .into_iter()
                .zip(prove_non_nullable_outputs(
                    &right.constant_analysis,
                    session,
                )?)
                .enumerate()
            {
                if constant && nonnull {
                    context.require_non_null(index);
                }
            }
        }
        let left = lower_sets(&set.left, context, session, state)?;
        context.leave_input(saved);
        let analyzed_markers = if set.is_all {
            vec![None; left.constant_analysis.schema().fields().len()]
        } else {
            build_set_result_constant_markers(&left.constant_analysis, session)?
        };
        // An enclosing predicate may discard NULL later, so it cannot tighten the executable field here.
        let mut executable_markers = analyzed_markers.clone();
        for (marker, nonnull) in executable_markers
            .iter_mut()
            .zip(prove_non_nullable_outputs(&left.executable, session)?)
        {
            if let Some(marker) = marker {
                marker.nullable = !nonnull;
            }
        }
        // Spark removes a group-only DISTINCT immediately below LIMIT 1.
        let under_limit_one = !set.is_all && context.has_single_row_limit()?;
        return Ok(SetLoweringResult {
            executable: lower_set_operation(
                left.executable,
                right.executable,
                set.join_type,
                set.is_all,
                &executable_markers,
                under_limit_one,
                state,
            )?,
            constant_analysis: lower_set_operation(
                left.constant_analysis,
                right.constant_analysis,
                set.join_type,
                set.is_all,
                &analyzed_markers,
                under_limit_one,
                state,
            )?,
        });
    }
    let mut executable_inputs = Vec::new();
    let mut analyzed_inputs = Vec::new();
    for (index, input) in plan.inputs().into_iter().enumerate() {
        let saved = context.enter_input(plan, index);
        let resolved = lower_sets(input, context, session, state)?;
        context.leave_input(saved);
        executable_inputs.push(resolved.executable);
        analyzed_inputs.push(resolved.constant_analysis);
    }
    let plan_with_subqueries = plan
        .clone()
        .map_subqueries(|subquery| {
            lower_sets(
                &subquery,
                &mut EnclosingQueryContext::default(),
                session,
                state,
            )
            .map(|resolved| Transformed::yes(resolved.executable))
        })?
        .data;
    let mut executable = replace_inputs(plan_with_subqueries.clone(), executable_inputs)?;
    let mut analyzed = replace_inputs(plan_with_subqueries, analyzed_inputs)?;
    if let LogicalPlan::Join(original) = plan
        && let LogicalPlan::Join(join) = &mut analyzed
    {
        join.join_type = context.effective_join_type(original, session)?;
    }
    if let LogicalPlan::Aggregate(analysis) = &mut analyzed
        && !analysis
            .group_expr
            .iter()
            .any(|expr| matches!(expr, Expr::GroupingSet(_)))
        && let LogicalPlan::Aggregate(actual) = &mut executable
    {
        let constants = constant_output_columns(&analysis.input)?;
        for (actual_key, analyzed_key) in actual.group_expr.iter_mut().zip(&mut analysis.group_expr)
        {
            if expression_uses_only_constants(analyzed_key, analysis.input.schema(), &constants)? {
                *actual_key = retain_constant_group_value(actual_key.clone());
                *analyzed_key = retain_constant_group_value(analyzed_key.clone());
            }
        }
    }
    Ok(SetLoweringResult {
        executable,
        constant_analysis: analyzed.recompute_schema()?,
    })
}

fn retain_constant_group_value(expr: Expr) -> Expr {
    match expr {
        Expr::Alias(mut alias) => {
            alias.expr = Box::new(retain_constant_group_value(*alias.expr));
            Expr::Alias(alias)
        }
        Expr::ScalarFunction(mut function)
            if function.func.inner().is::<SparkComparisonFloat>() =>
        {
            function.args.remove(0)
        }
        expr => expr,
    }
}

fn replace_inputs(plan: LogicalPlan, inputs: Vec<LogicalPlan>) -> Result<LogicalPlan> {
    let mut inputs = inputs.into_iter();
    Ok(plan
        .map_children(|_| {
            inputs.next().map(Transformed::yes).ok_or_else(|| {
                datafusion_common::DataFusionError::Internal("Missing set-analysis input".into())
            })
        })?
        .data)
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct PendingSetOperation {
    pub(super) left: Arc<LogicalPlan>,
    pub(super) right: Arc<LogicalPlan>,
    pub(super) join_type: JoinType,
    pub(super) is_all: bool,
    schema: DFSchemaRef,
}

impl PartialOrd for PendingSetOperation {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        (&self.left, &self.right, self.join_type, self.is_all).partial_cmp(&(
            &other.left,
            &other.right,
            other.join_type,
            other.is_all,
        ))
    }
}

impl PendingSetOperation {
    pub(super) fn new(
        left: LogicalPlan,
        right: LogicalPlan,
        join_type: JoinType,
        is_all: bool,
    ) -> Result<Self> {
        let fields = left
            .schema()
            .iter()
            .map(|(qualifier, field)| {
                let field = if !is_all
                    && matches!(field.data_type(), DataType::Float32 | DataType::Float64)
                {
                    Arc::new(field.as_ref().clone().with_nullable(true))
                } else {
                    field.clone()
                };
                (qualifier.cloned(), field)
            })
            .collect();
        let schema = Arc::new(DFSchema::new_with_metadata(
            fields,
            left.schema().metadata().clone(),
        )?);
        Ok(Self {
            left: Arc::new(left),
            right: Arc::new(right),
            join_type,
            is_all,
            schema,
        })
    }
}

impl UserDefinedLogicalNodeCore for PendingSetOperation {
    fn name(&self) -> &str {
        "PendingSetOperation"
    }
    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.left, &self.right]
    }
    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }
    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }
    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "PendingSetOperation: {:?}, all={}",
            self.join_type, self.is_all
        )
    }
    fn with_exprs_and_inputs(&self, exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        let [left, right]: [LogicalPlan; 2] = inputs.try_into().map_err(|_| {
            datafusion_common::DataFusionError::Internal("Set operation requires two inputs".into())
        })?;
        if !exprs.is_empty() {
            return internal_err!("Set operation has no expressions");
        }
        Self::new(left, right, self.join_type, self.is_all)
    }
}

#[cfg(test)]
mod tests {
    use super::super::constant_columns::PreserveSetConstant;

    #[tokio::test]
    async fn table_function_input_contains_an_executable_set_plan() -> crate::error::PlanResult<()>
    {
        use crate::resolver::PlanResolver;
        use arrow::array::Float64Array;
        use datafusion::catalog::{TableFunctionImpl, TableProvider};
        use datafusion::datasource::view::ViewTable;
        use datafusion::physical_plan::collect;
        use sail_function::scalar::table_input::TableInput;

        #[derive(Debug)]
        struct PassTable;
        impl TableFunctionImpl for PassTable {
            fn call(&self, args: &[Expr]) -> Result<Arc<dyn TableProvider>> {
                let [Expr::ScalarFunction(function)] = args else {
                    return internal_err!("Expected one table input");
                };
                let Some(input) = function.func.inner().downcast_ref::<TableInput>() else {
                    return internal_err!("Expected the table-input placeholder");
                };
                Ok(Arc::new(ViewTable::new(
                    input.plan().as_ref().clone(),
                    None,
                )))
            }
        }
        let session = SessionContext::new();
        session.register_udtf("pass_table", Arc::new(PassTable));
        let sql = "SELECT * FROM pass_table(TABLE(SELECT CAST('-0.0' AS DOUBLE) AS k INTERSECT SELECT CAST('0.0' AS DOUBLE) AS k))";
        let ast = sail_sql_analyzer::parser::parse_one_statement(sql)?;
        let query = sail_sql_analyzer::statement::from_ast_statement(ast)?;
        let named = PlanResolver::new(&session, Arc::new(Default::default()))
            .resolve_named_plan(query)
            .await?;
        let physical = session.state().create_physical_plan(&named.plan).await?;
        let batches = collect(physical, session.task_ctx()).await?;
        let values = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .iter()
            })
            .collect::<Vec<_>>();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0].unwrap().to_bits(), (-0.0_f64).to_bits());
        Ok(())
    }
    use super::*;
    use datafusion::prelude::SessionContext;
    use datafusion_common::{Column, NullEquality};
    use datafusion_expr::{Extension, Join, JoinConstraint, LogicalPlanBuilder, col, lit};

    #[test]
    fn full_set_lowering_keeps_nullable_projection_diamonds_linear() -> Result<()> {
        let session = SessionContext::new().state();
        let mut sizes = Vec::new();
        for depth in [8, 40] {
            let left = LogicalPlanBuilder::empty(true)
                .project([lit(1).alias("left_id")])?
                .build()?;
            let right = LogicalPlanBuilder::empty(true)
                .project([
                    lit(2).alias("right_id"),
                    lit(-0.0_f64).alias("a"),
                    lit(-0.0_f64).alias("b"),
                ])?
                .build()?;
            let join = LogicalPlan::Join(Join::try_new(
                Arc::new(left),
                Arc::new(right),
                vec![(col("left_id"), col("right_id"))],
                None,
                JoinType::Left,
                JoinConstraint::On,
                NullEquality::NullEqualsNothing,
                false,
            )?);
            let mut input = LogicalPlanBuilder::from(join)
                .project([col("a"), col("b")])?
                .build()?;
            for _ in 0..depth {
                input = LogicalPlanBuilder::from(input)
                    .project([
                        (col("a") + col("b")).alias("a"),
                        (col("a") + col("b")).alias("b"),
                    ])?
                    .build()?;
            }
            let right = LogicalPlanBuilder::empty(true)
                .project([lit(0.0_f64).alias("right_a"), lit(0.0_f64).alias("right_b")])?
                .build()?;
            let plan = LogicalPlan::Extension(Extension {
                node: Arc::new(PendingSetOperation::new(
                    input,
                    right,
                    JoinType::LeftSemi,
                    false,
                )?),
            });
            let result = lower_query_sets(&plan, session.clone(), &mut PlanResolverState::new())?;
            let mut expressions = 0;
            let mut preserved = 0;
            result.apply_with_subqueries(|plan| {
                assert!(!matches!(plan, LogicalPlan::Extension(extension)
                    if extension.node.as_any().is::<PendingSetOperation>()));
                for expr in plan.expressions() {
                    expr.apply(|expr| {
                        expressions += 1;
                        if matches!(expr, Expr::ScalarFunction(function) if function.func.inner().is::<PreserveSetConstant>()) {
                            preserved += 1;
                        }
                        Ok(TreeNodeRecursion::Continue)
                    })?;
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
            assert_eq!(
                preserved, 2,
                "NULL rejection must restore both constant outputs"
            );
            assert_eq!(
                result.schema().columns(),
                vec![Column::from_name("a"), Column::from_name("b")]
            );
            sizes.push(expressions);
        }
        assert!(sizes[1] <= sizes[0] * 5, "expression counts: {sizes:?}");
        Ok(())
    }
}
