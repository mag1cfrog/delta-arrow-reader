//! Lower Spark signed displacements that native frame-shape validation rejects.

use std::fmt;
use std::sync::Arc;

use datafusion::execution::SessionState;
use datafusion::physical_expr::{
    create_physical_expr, create_physical_exprs, create_physical_sort_exprs,
};
use datafusion::physical_plan::windows::{self, BoundedWindowAggExec, WindowAggExec};
use datafusion::physical_plan::{ExecutionPlan, InputOrderMode, WindowExpr};
use datafusion::physical_planner::create_window_expr;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{DFSchema, DFSchemaRef, Result, internal_err};
use datafusion_expr::expr::{NullTreatment, WindowFunction};
use datafusion_expr::logical_plan::{Extension, Window};
use datafusion_expr::{
    Expr, ExprSchemable, LogicalPlan, UserDefinedLogicalNodeCore, WindowFrameBound,
};

pub(super) fn lower_signed_range_windows(plan: LogicalPlan) -> Result<LogicalPlan> {
    Ok(plan
        .transform_up_with_subqueries(|plan| {
            let LogicalPlan::Window(window) = plan else {
                return Ok(Transformed::no(plan));
            };
            for expression in &window.window_expr {
                if requires_signed_range_planning(expression, &window.schema)? {
                    return Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                        node: Arc::new(SparkSignedRangeWindowNode { window }),
                    })));
                }
            }
            Ok(Transformed::no(LogicalPlan::Window(window)))
        })?
        .data)
}

fn window_function(expression: &Expr) -> Option<&WindowFunction> {
    match expression {
        Expr::Alias(alias) => window_function(&alias.expr),
        Expr::WindowFunction(function) => Some(function),
        _ => None,
    }
}

fn requires_signed_range_planning(expression: &Expr, schema: &DFSchema) -> Result<bool> {
    let Some(function) = window_function(expression) else {
        return Ok(false);
    };
    let frame = &function.params.window_frame;
    if !frame.has_nonzero_spark_integer_range_offset() || frame.has_null_offset() {
        return Ok(false);
    }
    let (WindowFrameBound::Following(displacement), WindowFrameBound::CurrentRow) =
        (&frame.start_bound, &frame.end_bound)
    else {
        return Ok(false);
    };
    let [order] = function.params.order_by.as_slice() else {
        return Ok(false);
    };
    let key_type = order.expr.get_type(schema)?;
    Ok(!displacement.is_null()
        && key_type.is_signed_integer()
        && displacement.data_type() == key_type)
}

/// Retain an optimized Window without changing its admitted frame bounds.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd)]
pub(super) struct SparkSignedRangeWindowNode {
    window: Window,
}

impl UserDefinedLogicalNodeCore for SparkSignedRangeWindowNode {
    fn name(&self) -> &str {
        "SparkSignedRangeWindow"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![self.window.input.as_ref()]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.window.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        self.window.window_expr.clone()
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SparkSignedRangeWindow: ")?;
        for (index, expression) in self.window.window_expr.iter().enumerate() {
            if index > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{expression}")?;
        }
        Ok(())
    }

    fn with_exprs_and_inputs(
        &self,
        expressions: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> Result<Self> {
        if inputs.len() != 1 {
            return internal_err!("SparkSignedRangeWindow requires exactly one logical input");
        }
        Ok(Self {
            window: Window::try_new(expressions, Arc::new(inputs.remove(0)))?,
        })
    }
}

pub(super) fn plan_signed_range_window(
    node: &SparkSignedRangeWindowNode,
    input: Arc<dyn ExecutionPlan>,
    state: &SessionState,
) -> Result<Arc<dyn ExecutionPlan>> {
    if node.window.window_expr.is_empty() {
        return internal_err!("SparkSignedRangeWindow requires at least one window expression");
    }
    let expressions = node
        .window
        .window_expr
        .iter()
        .map(|expression| {
            if requires_signed_range_planning(expression, &node.window.schema)? {
                create_signed_range_window_expr(expression, &node.window.schema, state)
            } else {
                create_window_expr(expression, &node.window.schema, state.execution_props())
            }
        })
        .collect::<Result<Vec<_>>>()?;
    let can_repartition =
        state.config().target_partitions() > 1 && state.config().repartition_window_functions();
    if expressions
        .iter()
        .all(|expression| expression.uses_bounded_memory())
    {
        Ok(Arc::new(BoundedWindowAggExec::try_new(
            expressions,
            input,
            InputOrderMode::Sorted,
            can_repartition,
        )?))
    } else {
        Ok(Arc::new(WindowAggExec::try_new(
            expressions,
            input,
            can_repartition,
        )?))
    }
}

fn create_signed_range_window_expr(
    expression: &Expr,
    schema: &DFSchema,
    state: &SessionState,
) -> Result<Arc<dyn WindowExpr>> {
    let (name, expression) = match expression {
        Expr::Alias(alias) => (
            alias.name.clone(),
            alias.expr.as_ref().clone().unalias_nested().data,
        ),
        _ => (expression.schema_name().to_string(), expression.clone()),
    };
    let Expr::WindowFunction(function) = expression else {
        return internal_err!("SparkSignedRangeWindow requires a window function");
    };
    let params = &function.params;
    let execution_props = state.execution_props();
    // Keep native expression conversion; only its frame-shape check is bypassed.
    let args = create_physical_exprs(&params.args, schema, execution_props)?;
    let partition_by = create_physical_exprs(&params.partition_by, schema, execution_props)?;
    let order_by = create_physical_sort_exprs(&params.order_by, schema, execution_props)?;
    let filter = params
        .filter
        .as_ref()
        .map(|filter| create_physical_expr(filter, schema, execution_props))
        .transpose()?;
    windows::create_window_expr(
        &function.fun,
        name,
        &args,
        &partition_by,
        &order_by,
        Arc::new(params.window_frame.clone()),
        Arc::clone(schema.inner()),
        params.null_treatment.unwrap_or(NullTreatment::RespectNulls) == NullTreatment::IgnoreNulls,
        params.distinct,
        filter,
    )
}
