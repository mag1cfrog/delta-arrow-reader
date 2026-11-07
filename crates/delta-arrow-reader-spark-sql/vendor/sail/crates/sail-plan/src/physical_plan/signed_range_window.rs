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

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{ArrayRef, Int32Array, Int64Array};
    use arrow::compute::concat_batches;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::execution::TaskContext;
    use datafusion::functions_aggregate::count::count_udaf;
    use datafusion::functions_window::nth_value::first_value_udwf;
    use datafusion::physical_plan::test::TestMemoryExec;
    use datafusion::prelude::SessionContext;
    use datafusion_common::{ScalarValue, internal_datafusion_err};
    use datafusion_expr::window_frame::RangeOffsetArithmetic;
    use datafusion_expr::{
        EmptyRelation, WindowFrame, WindowFrameUnits, WindowFunctionDefinition, col, lit,
    };

    fn input_schema(key_type: DataType) -> Schema {
        Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("k", key_type, false),
            Field::new("v", DataType::Int32, true),
        ])
    }

    fn signed_frame(displacement: ScalarValue) -> WindowFrame {
        WindowFrame::new_bounds(
            WindowFrameUnits::Range,
            WindowFrameBound::Following(displacement),
            WindowFrameBound::CurrentRow,
        )
        .with_range_offset_arithmetic(RangeOffsetArithmetic::Spark { ansi_mode: true })
    }

    fn count_function(frame: WindowFrame) -> WindowFunction {
        let mut function = WindowFunction::new(count_udaf(), vec![col("v")]);
        function.params.order_by = vec![col("k").sort(true, false)];
        function.params.window_frame = frame;
        function
    }

    fn alias_window_function(function: WindowFunction, name: &str) -> Expr {
        Expr::WindowFunction(Box::new(function)).alias(name)
    }

    #[test]
    fn signed_range_lowering_selects_only_the_admitted_shape_and_rebuilds_schema() -> Result<()> {
        for displacement in [
            ScalarValue::Int8(Some(-1)),
            ScalarValue::Int16(Some(-1)),
            ScalarValue::Int32(Some(-1)),
            ScalarValue::Int64(Some(-1)),
        ] {
            let schema = Arc::new(DFSchema::try_from(input_schema(displacement.data_type()))?);
            let frame = signed_frame(displacement);
            let selected = alias_window_function(count_function(frame.clone()), "selected");
            assert!(requires_signed_range_planning(&selected, &schema)?);
            let input = Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: false,
                schema,
            }));
            let window = Window::try_new(vec![selected.clone()], Arc::clone(&input))?;
            let expected_schema = Arc::clone(&window.schema);
            let lowered = lower_signed_range_windows(LogicalPlan::Window(window))?;
            let LogicalPlan::Extension(extension) = lowered else {
                return internal_err!("signed displacement was not lowered");
            };
            let node = extension
                .node
                .as_any()
                .downcast_ref::<SparkSignedRangeWindowNode>()
                .ok_or_else(|| {
                    internal_datafusion_err!("lowered extension is not SparkSignedRangeWindow")
                })?;
            assert_eq!(node.schema(), &expected_schema);
            assert_eq!(node.expressions(), vec![selected]);
            assert_eq!(node.inputs(), vec![input.as_ref()]);
            assert!(
                node.with_exprs_and_inputs(node.expressions(), vec![])
                    .is_err()
            );
            let renamed = alias_window_function(count_function(frame.clone()), "renamed");
            let rebuilt =
                node.with_exprs_and_inputs(vec![renamed], vec![input.as_ref().clone()])?;
            let renamed_field =
                rebuilt.schema().fields().last().ok_or_else(|| {
                    internal_datafusion_err!("rebuilt window has no output Field")
                })?;
            assert_eq!(renamed_field.name(), "renamed");
            let rebuilt_expressions = rebuilt.expressions();
            let rebuilt_function = window_function(&rebuilt_expressions[0]).ok_or_else(|| {
                internal_datafusion_err!("rebuilt expression is not a window function")
            })?;
            assert_eq!(rebuilt_function.params.window_frame, frame);
        }

        let schema = DFSchema::try_from(input_schema(DataType::Int32))?;
        let selected = signed_frame(ScalarValue::Int32(Some(-1)));
        assert!(requires_signed_range_planning(
            &alias_window_function(
                count_function(signed_frame(ScalarValue::Int32(Some(1)))),
                "positive"
            ),
            &schema,
        )?);
        let mut excluded = vec![
            selected
                .clone()
                .with_range_offset_arithmetic(RangeOffsetArithmetic::DataFusion),
            signed_frame(ScalarValue::Int32(Some(0))),
            signed_frame(ScalarValue::Int32(None)),
            WindowFrame::new_bounds(
                WindowFrameUnits::Range,
                WindowFrameBound::FollowingNullOffset(DataType::Int32),
                WindowFrameBound::CurrentRow,
            )
            .with_range_offset_arithmetic(RangeOffsetArithmetic::Spark { ansi_mode: true }),
            WindowFrame::new_bounds(
                WindowFrameUnits::Range,
                WindowFrameBound::Following(ScalarValue::Int32(Some(2))),
                WindowFrameBound::Following(ScalarValue::Int32(Some(1))),
            )
            .with_range_offset_arithmetic(RangeOffsetArithmetic::Spark { ansi_mode: true }),
        ];
        for units in [WindowFrameUnits::Rows, WindowFrameUnits::Groups] {
            let mut frame = selected.clone();
            frame.units = units;
            excluded.push(frame);
        }
        for frame in excluded {
            assert!(!requires_signed_range_planning(
                &alias_window_function(count_function(frame), "excluded"),
                &schema
            )?);
        }
        for key_type in [DataType::UInt32, DataType::Date32, DataType::Int64] {
            let schema = DFSchema::try_from(input_schema(key_type))?;
            assert!(!requires_signed_range_planning(
                &alias_window_function(count_function(selected.clone()), "excluded"),
                &schema
            )?);
        }
        for order_by in [
            vec![],
            vec![col("k").sort(true, false), col("id").sort(true, false)],
        ] {
            let mut function = count_function(selected.clone());
            function.params.order_by = order_by;
            assert!(!requires_signed_range_planning(
                &alias_window_function(function, "excluded"),
                &schema
            )?);
        }
        Ok(())
    }

    #[tokio::test]
    async fn signed_range_planning_preserves_aliases_options_and_native_siblings() -> Result<()> {
        let schema = Arc::new(input_schema(DataType::Int32));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![0, 1, 2, 3])) as ArrayRef,
                Arc::new(Int32Array::from(vec![1, 2, 2, 3])),
                Arc::new(Int32Array::from(vec![None, Some(10), Some(10), Some(20)])),
            ],
        )?;
        let logical_input = Arc::new(LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::try_from(schema.as_ref().clone())?),
        }));
        let frame = signed_frame(ScalarValue::Int32(Some(-1)));
        let mut filtered_distinct = count_function(frame.clone());
        // Preserve the converter's fields without admitting new DISTINCT SQL syntax.
        filtered_distinct.params.distinct = true;
        filtered_distinct.params.filter = Some(Box::new(col("v").lt(lit(20_i32))));
        let mut first_nonnull = count_function(frame.clone());
        first_nonnull.fun = WindowFunctionDefinition::WindowUDF(first_value_udwf());
        first_nonnull.params.null_treatment = Some(NullTreatment::IgnoreNulls);
        let native_frame = WindowFrame::new_bounds(
            WindowFrameUnits::Range,
            WindowFrameBound::CurrentRow,
            WindowFrameBound::Following(ScalarValue::Int32(Some(1))),
        );
        let mut native_count = count_function(native_frame.clone());
        native_count.params.args = vec![lit(1_i32)];
        let node = SparkSignedRangeWindowNode {
            window: Window::try_new(
                vec![
                    alias_window_function(filtered_distinct, "filtered_distinct"),
                    alias_window_function(first_nonnull, "first_nonnull"),
                    alias_window_function(native_count, "native_count"),
                ],
                Arc::clone(&logical_input),
            )?,
        };
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(TestMemoryExec::try_new(&[vec![batch]], schema, None)?);
        let state = SessionContext::new().state();
        let plan = plan_signed_range_window(&node, Arc::clone(&input), &state)?;
        assert_eq!(plan.schema().as_ref(), node.schema().inner().as_ref());
        let bounded = plan
            .downcast_ref::<BoundedWindowAggExec>()
            .ok_or_else(|| internal_datafusion_err!("finite functions did not remain bounded"))?;
        assert_eq!(
            bounded
                .window_expr()
                .iter()
                .map(|expr| expr.name())
                .collect::<Vec<_>>(),
            vec!["filtered_distinct", "first_nonnull", "native_count"]
        );
        assert_eq!(bounded.window_expr()[0].get_window_frame().as_ref(), &frame);
        assert_eq!(bounded.window_expr()[1].get_window_frame().as_ref(), &frame);
        assert_eq!(
            bounded.window_expr()[2].get_window_frame().as_ref(),
            &native_frame
        );
        let output_schema = plan.schema();
        let batches =
            datafusion::physical_plan::collect(plan, Arc::new(TaskContext::default())).await?;
        let output = concat_batches(&output_schema, &batches)?;
        let filtered_distinct_output = output
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| internal_datafusion_err!("filtered distinct output is not Int64"))?;
        assert_eq!(
            filtered_distinct_output,
            &Int64Array::from(vec![0, 1, 1, 1])
        );
        let first_nonnull_output = output
            .column(4)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| internal_datafusion_err!("first nonnull output is not Int32"))?;
        assert_eq!(
            first_nonnull_output,
            &Int32Array::from(vec![None, Some(10), Some(10), Some(10)])
        );
        let native_count_output = output
            .column(5)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| internal_datafusion_err!("native count output is not Int64"))?;
        assert_eq!(native_count_output, &Int64Array::from(vec![3, 3, 3, 1]));

        let invalid_native = signed_frame(ScalarValue::Int32(Some(1)))
            .with_range_offset_arithmetic(RangeOffsetArithmetic::DataFusion);
        let mut mixed = node.clone();
        mixed.window = Window::try_new(
            vec![
                node.window.window_expr[0].clone(),
                alias_window_function(count_function(invalid_native), "invalid_native"),
            ],
            logical_input,
        )?;
        let error = match plan_signed_range_window(&mixed, input, &state) {
            Ok(_) => return internal_err!("invalid native sibling unexpectedly planned"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("Invalid window frame"));
        Ok(())
    }
}
