// Modified from Sail v0.7.1 for the Delta reader experiment. See experiments/spark-sql/UPSTREAM.md in the host repository.
use std::cmp::Ordering;

use arrow::datatypes::DataType;
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion_common::{DFSchemaRef, ScalarValue};
use datafusion_expr::simplify::SimplifyContextBuilder;
use datafusion_expr::{ExprSchemable, WindowFrame, WindowFrameBound, WindowFrameUnits, expr};
use sail_common::spec::{self};
use sail_common_datafusion::literal::LiteralEvaluator;
use sail_common_datafusion::utils::items::ItemTaker;

use crate::error::{PlanError, PlanResult};
use crate::formatter::SparkPlanFormatter;
use crate::function::common::{FunctionContextInput, WinFunctionInput};
use crate::function::get_built_in_window_function;
use crate::resolver::PlanResolver;
use crate::resolver::expression::NamedExpr;
use crate::resolver::state::PlanResolverState;

impl PlanResolver<'_> {
    pub(super) async fn resolve_expression_window(
        &self,
        window_function: spec::Expr,
        window: spec::Window,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<NamedExpr> {
        let window = match window {
            spec::Window::Named(name) => state
                .get_window(name.as_ref())
                .ok_or_else(|| PlanError::analysis(format!("undefined window: {}", name.as_ref())))?
                .clone(),
            w => w,
        };

        let spec::Window::Unnamed {
            cluster_by,
            partition_by,
            order_by,
            frame,
        } = window
        else {
            return Err(PlanError::analysis("named windows in window expressions"));
        };
        if !cluster_by.is_empty() {
            return Err(PlanError::unsupported(
                "CLUSTER BY clause in window expression",
            ));
        }
        let partition_by = self
            .resolve_expressions(partition_by, schema, state)
            .await?;
        // Spark treats literals as constants in ORDER BY window definition
        let sorts = self
            .resolve_sort_orders(order_by, false, schema, state)
            .await?;
        let window_frame = if let Some(frame) = frame {
            self.resolve_window_frame(frame, &sorts, schema, state)
                .await?
        } else {
            WindowFrame::new(if sorts.is_empty() {
                None
            } else {
                // TODO: should we use strict ordering or not?
                Some(false)
            })
        };
        let (window, function_name, argument_display_names, is_distinct) = match window_function {
            spec::Expr::UnresolvedFunction(spec::UnresolvedFunction {
                function_name,
                arguments,
                named_arguments,
                is_user_defined_function: false,
                is_internal: _,
                is_distinct,
                ignore_nulls,
                filter: None,
                // TODO: `window` and `window_function` both have an `order_by` field.
                //  Check: Which one should we use? Are they the same? Is one of them empty?
                order_by: None,
            }) => {
                let Ok(function_name) = <Vec<String>>::from(function_name).one() else {
                    return Err(PlanError::unsupported("qualified window function name"));
                };
                if !named_arguments.is_empty() {
                    return Err(PlanError::todo("named window function arguments"));
                }
                let canonical_function_name = function_name.to_ascii_lowercase();
                let (argument_display_names, arguments) = self
                    .resolve_expressions_and_names(arguments, schema, state)
                    .await?;
                let function = get_built_in_window_function(&canonical_function_name)?;
                let input = WinFunctionInput {
                    arguments,
                    partition_by,
                    order_by: sorts,
                    window_frame,
                    ignore_nulls,
                    distinct: is_distinct,
                    function_context: FunctionContextInput {
                        argument_display_names: &argument_display_names,
                        plan_config: &self.config,
                        session_context: self.ctx,
                        schema,
                    },
                };
                (
                    function(input)?,
                    function_name,
                    argument_display_names,
                    is_distinct,
                )
            }
            spec::Expr::CommonInlineUserDefinedFunction { .. } => {
                return Err(PlanError::unsupported(
                    "inline user-defined window functions",
                ));
            }
            _ => {
                return Err(PlanError::invalid(format!(
                    "invalid window function expression: {window_function:?}"
                )));
            }
        };

        let name = SparkPlanFormatter.function_to_string(
            function_name.as_str(),
            argument_display_names.iter().map(|x| x.as_str()).collect(),
            is_distinct,
        )?;
        Ok(NamedExpr::new(vec![name], window))
    }

    async fn resolve_window_frame(
        &self,
        frame: spec::WindowFrame,
        order_by: &[expr::Sort],
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<WindowFrame> {
        use spec::WindowFrameType;

        let spec::WindowFrame {
            frame_type,
            lower,
            upper,
        } = frame;

        let units = match frame_type {
            WindowFrameType::Row => WindowFrameUnits::Rows,
            WindowFrameType::Range => WindowFrameUnits::Range,
        };
        let (start, end) = match units {
            WindowFrameUnits::Rows | WindowFrameUnits::Groups => (
                self.resolve_window_boundary_offset(lower, schema, state)
                    .await?,
                self.resolve_window_boundary_offset(upper, schema, state)
                    .await?,
            ),
            WindowFrameUnits::Range => (
                self.resolve_window_boundary_value(lower, order_by, schema, state)
                    .await?,
                self.resolve_window_boundary_value(upper, order_by, schema, state)
                    .await?,
            ),
        };
        Ok(WindowFrame::new_bounds(units, start, end))
    }

    async fn resolve_window_boundary(
        &self,
        expr: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<ScalarValue> {
        if let spec::Expr::Literal(value) = expr {
            return self.resolve_literal(value, state);
        }
        let resolved = self.resolve_expression(expr, schema, state).await?;
        if let datafusion_expr::Expr::Literal(scalar, _) = resolved {
            return Ok(scalar);
        }
        // Apply type coercion so that expressions like `CAST(0 AS INTERVAL SECOND)`
        // have compatible types before physical evaluation.
        let context = SimplifyContextBuilder::default()
            .with_schema(schema.clone())
            .build();
        let simplifier = ExprSimplifier::new(context);
        let coerced = simplifier.coerce(resolved, schema).map_err(|e| {
            PlanError::invalid(format!(
                "window boundary must be a constant expression: {e}"
            ))
        })?;
        let evaluator = LiteralEvaluator::new();
        evaluator.evaluate(&coerced).map_err(|e| {
            PlanError::invalid(format!(
                "window boundary must be a constant expression: {e}"
            ))
        })
    }

    async fn resolve_window_boundary_offset(
        &self,
        value: spec::WindowFrameBoundary,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<WindowFrameBound> {
        match value {
            spec::WindowFrameBoundary::CurrentRow => Ok(WindowFrameBound::CurrentRow),
            spec::WindowFrameBoundary::UnboundedPreceding => {
                Ok(WindowFrameBound::Preceding(ScalarValue::UInt64(None)))
            }
            spec::WindowFrameBoundary::UnboundedFollowing => {
                Ok(WindowFrameBound::Following(ScalarValue::UInt64(None)))
            }
            spec::WindowFrameBoundary::Preceding(expr) => {
                let value = self.resolve_window_boundary(*expr, schema, state).await?;
                Ok(WindowFrameBound::Preceding(
                    value.cast_to(&DataType::UInt64)?,
                ))
            }
            spec::WindowFrameBoundary::Following(expr) => {
                let value = self.resolve_window_boundary(*expr, schema, state).await?;
                Ok(WindowFrameBound::Following(
                    value.cast_to(&DataType::UInt64)?,
                ))
            }
            spec::WindowFrameBoundary::Value(expr) => {
                let value = self.resolve_window_boundary(*expr, schema, state).await?;
                let ScalarValue::Int64(Some(value)) = value.cast_to(&DataType::Int64)? else {
                    return Err(PlanError::invalid("invalid window boundary offset"));
                };
                match value {
                    i64::MIN => Ok(WindowFrameBound::Preceding(ScalarValue::UInt64(None))),
                    i64::MAX => Ok(WindowFrameBound::Following(ScalarValue::UInt64(None))),
                    0 => Ok(WindowFrameBound::CurrentRow),
                    x if x < 0 => Ok(WindowFrameBound::Preceding(ScalarValue::UInt64(Some(
                        -x as u64,
                    )))),
                    x => Ok(WindowFrameBound::Following(ScalarValue::UInt64(Some(
                        x as u64,
                    )))),
                }
            }
        }
    }

    async fn resolve_window_boundary_value(
        &self,
        value: spec::WindowFrameBoundary,
        order_by: &[expr::Sort],
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<WindowFrameBound> {
        // Helper to get the data type from the order_by expression for casting
        let get_order_by_type = || -> PlanResult<DataType> {
            let [order_by] = order_by else {
                return Err(PlanError::invalid(
                    "range window frame requires exactly one order by expression",
                ));
            };
            Ok(order_by.expr.to_field(schema)?.1.data_type().clone())
        };

        match value {
            spec::WindowFrameBoundary::CurrentRow => Ok(WindowFrameBound::CurrentRow),
            spec::WindowFrameBoundary::UnboundedPreceding => {
                Ok(WindowFrameBound::Preceding(ScalarValue::Null))
            }
            spec::WindowFrameBoundary::UnboundedFollowing => {
                Ok(WindowFrameBound::Following(ScalarValue::Null))
            }
            spec::WindowFrameBoundary::Preceding(expr) => {
                let value = self.resolve_window_boundary(*expr, schema, state).await?;
                // Cast numeric boundaries to match the ORDER BY type.
                // Non-numeric boundaries (e.g. INTERVAL for TIMESTAMP ORDER BY) are left as-is
                // since DataFusion handles interval arithmetic directly.
                let data_type = get_order_by_type()?;
                let value = if data_type.is_numeric() {
                    value.cast_to(&data_type)?
                } else {
                    value
                };
                Ok(WindowFrameBound::Preceding(value))
            }
            spec::WindowFrameBoundary::Following(expr) => {
                let value = self.resolve_window_boundary(*expr, schema, state).await?;
                let data_type = get_order_by_type()?;
                let value = if data_type.is_numeric() {
                    value.cast_to(&data_type)?
                } else {
                    value
                };
                Ok(WindowFrameBound::Following(value))
            }
            spec::WindowFrameBoundary::Value(expr) => {
                let value = self.resolve_window_boundary(*expr, schema, state).await?;
                if value.is_null() {
                    Err(PlanError::invalid("window boundary value cannot be null"))
                } else {
                    let data_type = get_order_by_type()?;
                    let value = if data_type.is_numeric() {
                        value.cast_to(&data_type)?
                    } else {
                        value
                    };
                    let zero = ScalarValue::new_zero(&data_type)?;
                    match value.partial_cmp(&zero) {
                        None => Err(PlanError::invalid(
                            "cannot compare window boundary value with zero",
                        )),
                        Some(Ordering::Less) => {
                            let value = value.arithmetic_negate()?;
                            Ok(WindowFrameBound::Preceding(value))
                        }
                        Some(Ordering::Greater) => Ok(WindowFrameBound::Following(value)),
                        Some(Ordering::Equal) => Ok(WindowFrameBound::CurrentRow),
                    }
                }
            }
        }
    }
}
