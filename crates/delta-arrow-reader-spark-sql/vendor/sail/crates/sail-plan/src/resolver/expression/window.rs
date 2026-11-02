// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
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
use crate::function::{get_built_in_window_function, require_deterministic_scalar_expression};
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
        let (window, function_name, argument_display_names, is_distinct) = match window_function {
            spec::Expr::UnresolvedFunction(spec::UnresolvedFunction {
                function_name,
                arguments,
                named_arguments,
                is_user_defined_function: false,
                is_internal: _,
                is_distinct,
                ignore_nulls,
                filter,
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
                if is_distinct {
                    return Err(PlanError::unsupported(
                        "[DISTINCT_WINDOW_FUNCTION_UNSUPPORTED] DISTINCT window functions are unsupported",
                    ));
                }
                let canonical_function_name = function_name.to_ascii_lowercase();
                let (argument_display_names, arguments) = self
                    .resolve_expression_arguments(arguments, schema, state)
                    .await?;
                let function = get_built_in_window_function(&canonical_function_name)?;
                let required_units = match canonical_function_name.as_str() {
                    "row_number" | "rank" | "dense_rank" | "percent_rank" | "ntile" => {
                        Some(WindowFrameUnits::Rows)
                    }
                    "cume_dist" => Some(WindowFrameUnits::Range),
                    _ => None,
                };
                if let (Some(units), Some(frame)) = (required_units, frame.as_ref()) {
                    let expected_type = if units == WindowFrameUnits::Rows {
                        spec::WindowFrameType::Row
                    } else {
                        spec::WindowFrameType::Range
                    };
                    if frame.frame_type != expected_type
                        || frame.lower != spec::WindowFrameBoundary::UnboundedPreceding
                        || frame.upper != spec::WindowFrameBoundary::CurrentRow
                    {
                        return Err(PlanError::invalid(format!(
                            "[WINDOW_FRAME_MISMATCH] {function_name} requires {units} BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
                        )));
                    }
                }
                let offset_function = matches!(canonical_function_name.as_str(), "lag" | "lead");
                if offset_function && let Some(frame) = frame.as_ref() {
                    self.validate_offset_window_frame(
                        &canonical_function_name,
                        &arguments,
                        frame,
                        schema,
                        state,
                    )
                    .await?;
                }
                if sorts.is_empty()
                    && (required_units.is_some()
                        || offset_function
                        || canonical_function_name == "nth_value")
                {
                    return Err(PlanError::invalid(format!(
                        "[WINDOW_FUNCTION_FRAME_NOT_ORDERED] {function_name} requires ORDER BY in the window definition"
                    )));
                }
                let window_frame = if let Some(frame) = frame {
                    self.resolve_window_frame(frame, &sorts, schema, state)
                        .await?
                } else if let Some(units) = required_units {
                    WindowFrame::new_bounds(
                        units,
                        WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
                        WindowFrameBound::CurrentRow,
                    )
                } else {
                    WindowFrame::new((!sorts.is_empty()).then_some(false))
                };
                let filter = match filter {
                    Some(filter) => {
                        let filter = self.resolve_expression(*filter, schema, state).await?;
                        if filter.get_type(schema)? != DataType::Boolean {
                            return Err(PlanError::invalid("window FILTER requires BOOLEAN"));
                        }
                        require_deterministic_scalar_expression(&filter, "window FILTER")?;
                        Some(Box::new(filter))
                    }
                    None => None,
                };
                let input = WinFunctionInput {
                    arguments,
                    partition_by,
                    order_by: sorts,
                    window_frame,
                    ignore_nulls,
                    distinct: is_distinct,
                    filter,
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

    async fn validate_offset_window_frame(
        &self,
        function_name: &str,
        arguments: &[expr::Expr],
        frame: &spec::WindowFrame,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<()> {
        let mismatch = || {
            PlanError::invalid(format!(
                "[WINDOW_OFFSET_FRAME_MISMATCH] {function_name} frame must match its offset expression"
            ))
        };
        if frame.frame_type != spec::WindowFrameType::Row {
            return Err(mismatch());
        }
        let offset = arguments
            .get(1)
            .cloned()
            .unwrap_or_else(|| datafusion_expr::lit(1_i32));
        if offset.is_volatile()
            || !offset.column_refs().is_empty()
            || offset.contains_outer()
            || offset.contains_scalar_subquery()
        {
            return Err(mismatch());
        }
        // Spark folds LAG's negated offset but compares frame expressions before
        // folding them. Numerically equal frame expressions can still be invalid.
        let required = if function_name == "lag" {
            let context = SimplifyContextBuilder::default()
                .with_schema(schema.clone())
                .build();
            let coerced = ExprSimplifier::new(context)
                .coerce(expr::Expr::Negative(Box::new(offset)), schema)?;
            expr::Expr::Literal(LiteralEvaluator::new().evaluate(&coerced)?, None)
        } else {
            offset
        };
        for boundary in [&frame.lower, &frame.upper] {
            let actual = match boundary {
                spec::WindowFrameBoundary::Preceding(value) => expr::Expr::Negative(Box::new(
                    self.resolve_expression(*value.clone(), schema, state)
                        .await?,
                )),
                spec::WindowFrameBoundary::Following(value)
                | spec::WindowFrameBoundary::Value(value) => {
                    self.resolve_expression(*value.clone(), schema, state)
                        .await?
                }
                _ => return Err(mismatch()),
            };
            if actual != required {
                return Err(mismatch());
            }
        }
        Ok(())
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
        if units == WindowFrameUnits::Range && order_by.is_empty() {
            if lower != spec::WindowFrameBoundary::UnboundedPreceding
                || upper != spec::WindowFrameBoundary::UnboundedFollowing
            {
                return Err(PlanError::invalid(
                    "[DATATYPE_MISMATCH.RANGE_FRAME_WITHOUT_ORDER] a bounded RANGE frame requires ORDER BY",
                ));
            }
            // Native RANGE coercion requires an order key; the whole partition
            // has identical membership under ROWS when no key exists.
            return Ok(WindowFrame::new(None));
        }
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
        let frame = WindowFrame::new_bounds(units, start, end);
        if !datafusion::physical_planner::is_window_frame_bound_valid(&frame) {
            return Err(PlanError::invalid(format!(
                "[DATATYPE_MISMATCH.SPECIFIED_WINDOW_FRAME_WRONG_COMPARISON] start bound ({}) cannot be larger than end bound ({})",
                frame.start_bound, frame.end_bound,
            )));
        }
        Ok(frame)
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
        let preceding = matches!(value, spec::WindowFrameBoundary::Preceding(_));
        match value {
            spec::WindowFrameBoundary::CurrentRow => Ok(WindowFrameBound::CurrentRow),
            spec::WindowFrameBoundary::UnboundedPreceding => {
                Ok(WindowFrameBound::Preceding(ScalarValue::UInt64(None)))
            }
            spec::WindowFrameBoundary::UnboundedFollowing => {
                Ok(WindowFrameBound::Following(ScalarValue::UInt64(None)))
            }
            spec::WindowFrameBoundary::Preceding(expr)
            | spec::WindowFrameBoundary::Following(expr) => {
                let value = self.resolve_window_boundary(*expr, schema, state).await?;
                let ScalarValue::Int64(Some(offset)) = value.cast_to(&DataType::Int64)? else {
                    return Err(PlanError::invalid("invalid window boundary offset"));
                };
                if offset == 0 {
                    Ok(WindowFrameBound::CurrentRow)
                } else if preceding == (offset > 0) {
                    Ok(WindowFrameBound::Preceding(ScalarValue::UInt64(Some(
                        offset.unsigned_abs(),
                    ))))
                } else {
                    Ok(WindowFrameBound::Following(ScalarValue::UInt64(Some(
                        offset.unsigned_abs(),
                    ))))
                }
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
