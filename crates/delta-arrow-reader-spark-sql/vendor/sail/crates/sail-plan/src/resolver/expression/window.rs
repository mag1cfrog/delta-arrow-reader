// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::cmp::Ordering;

use arrow::compute::can_cast_types;
use arrow::datatypes::{DataType, FieldRef};
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion_common::tree_node::TreeNode;
use datafusion_common::{DFSchemaRef, DataFusionError, ScalarValue};
use datafusion_expr::simplify::SimplifyContextBuilder;
use datafusion_expr::window_frame::RangeOffsetArithmetic;
use datafusion_expr::{
    ColumnarValue, ExprSchemable, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl,
    Signature, Volatility, WindowFrame, WindowFrameBound, WindowFrameUnits, expr,
};
use sail_common::spec::{self};
use sail_common_datafusion::literal::LiteralEvaluator;
use sail_common_datafusion::utils::items::ItemTaker;
use sail_function::scalar::datetime::spark_date_day_arithmetic::SparkDateDayArithmetic;
use sail_function::scalar::math::spark_negative::SparkNegative;

use crate::error::{PlanError, PlanResult};
use crate::formatter::SparkPlanFormatter;
use crate::function::common::{FunctionContextInput, WinFunctionInput};
use crate::function::{get_built_in_window_function, require_deterministic_scalar_expression};
use crate::resolver::PlanResolver;
use crate::resolver::expression::NamedExpr;
use crate::resolver::expression::sort::sort_order_options;
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
        let (partition_display, partition_by) = self
            .resolve_expression_arguments(partition_by, schema, state)
            .await?;
        // Spark treats literals as constants in ORDER BY window definition
        let mut order_display_names = Vec::with_capacity(order_by.len());
        let mut sorts = Vec::with_capacity(order_by.len());
        for sort in order_by {
            let (names, expressions) = self
                .resolve_expression_arguments(vec![*sort.child], schema, state)
                .await?;
            let (asc, nulls_first) = sort_order_options(sort.direction, sort.null_ordering);
            order_display_names.push(names.one()?);
            sorts.push(expr::Sort {
                expr: expressions.one()?,
                asc,
                nulls_first,
            });
        }
        let (name, window) = match window_function {
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
                let (argument_display_names, mut arguments) = self
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
                let required_offset = if offset_function && let Some(frame) = frame.as_ref() {
                    Some(
                        self.validate_offset_window_frame(
                            &canonical_function_name,
                            &arguments,
                            frame,
                            schema,
                            state,
                        )
                        .await?,
                    )
                } else {
                    None
                };
                if sorts.is_empty()
                    && (required_units.is_some()
                        || offset_function
                        || canonical_function_name == "nth_value")
                {
                    return Err(PlanError::invalid(format!(
                        "[WINDOW_FUNCTION_FRAME_NOT_ORDERED] {function_name} requires ORDER BY in the window definition"
                    )));
                }
                let (mut frame_display, mut window_frame) = if let Some(frame) = frame {
                    self.resolve_window_frame(frame, &sorts, schema, state)
                        .await?
                } else if let Some(units) = required_units {
                    let frame = WindowFrame::new_bounds(
                        units,
                        WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
                        WindowFrameBound::CurrentRow,
                    );
                    (frame.to_string(), frame)
                } else {
                    let frame = WindowFrame::new((!sorts.is_empty()).then_some(false));
                    (frame.to_string(), frame)
                };
                let (filter_display, filter) = match filter {
                    Some(filter) => {
                        let (names, filters) = self
                            .resolve_expression_arguments(vec![*filter], schema, state)
                            .await?;
                        let filter = filters.one()?;
                        if filter.get_type(schema)? != DataType::Boolean {
                            return Err(PlanError::invalid("window FILTER requires BOOLEAN"));
                        }
                        require_deterministic_scalar_expression(&filter, "window FILTER")?;
                        (Some(names.one()?), Some(Box::new(filter)))
                    }
                    None => (None, None),
                };
                if offset_function && required_offset.is_none() {
                    let preflight_input = WinFunctionInput {
                        arguments: arguments.clone(),
                        partition_by: partition_by.clone(),
                        order_by: sorts.clone(),
                        window_frame: window_frame.clone(),
                        ignore_nulls,
                        distinct: is_distinct,
                        filter: filter.clone(),
                        function_context: FunctionContextInput {
                            argument_display_names: &argument_display_names,
                            plan_config: &self.config,
                            session_context: self.ctx,
                            schema,
                        },
                    };
                    function(preflight_input)?;
                }
                if offset_function {
                    let required = match required_offset {
                        Some(required) => required,
                        None => self.resolve_offset_window_boundary(
                            &canonical_function_name,
                            &arguments,
                            schema,
                        )?,
                    };
                    let offset_display = if canonical_function_name == "lag" {
                        if let Some(offset) = arguments.get_mut(1) {
                            // Reuse this fold instead of invoking the offset again during execution.
                            *offset = ScalarUDF::from(SparkNegative::new(self.config.ansi_mode))
                                .call(vec![required.clone()]);
                        }
                        let expr::Expr::Literal(value, _) = required else {
                            return Err(PlanError::invalid("literal LAG frame boundary expected"));
                        };
                        SparkPlanFormatter
                            .literal_to_string(&value, &self.config.session_timezone)?
                    } else {
                        argument_display_names
                            .get(1)
                            .cloned()
                            .unwrap_or_else(|| "1".into())
                    };
                    frame_display = format!(
                        "ROWS BETWEEN {offset_display} FOLLOWING AND {offset_display} FOLLOWING"
                    );
                }
                let nth_value_ignore_nulls = canonical_function_name == "nth_value"
                    && if arguments.len() == 3 {
                        matches!(
                            &arguments[2],
                            expr::Expr::Literal(ScalarValue::Boolean(Some(true)), _)
                        )
                    } else {
                        ignore_nulls == Some(true)
                    };
                let mut display_arguments = argument_display_names
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>();
                if offset_function {
                    if display_arguments.len() == 1 {
                        display_arguments.push("1");
                    }
                    if display_arguments.len() == 2 {
                        display_arguments.push("NULL");
                    }
                } else if canonical_function_name == "nth_value" {
                    display_arguments.truncate(2);
                }
                let display_function_name = match canonical_function_name.as_str() {
                    "rank" => "RANK",
                    "dense_rank" => "DENSE_RANK",
                    "percent_rank" => "PERCENT_RANK",
                    name => name,
                };
                let name = SparkPlanFormatter
                    .function_to_string(display_function_name, display_arguments, is_distinct)
                    .map(|mut function_display| {
                        if nth_value_ignore_nulls {
                            function_display.push_str(" ignore nulls");
                        }
                        if let Some(filter) = filter_display {
                            function_display.push_str(&format!(" FILTER (WHERE {filter})"));
                        }
                        SparkPlanFormatter.window_expression_to_string(
                            &function_display,
                            &partition_display,
                            &order_display_names
                                .iter()
                                .zip(&sorts)
                                .map(|(name, sort)| (name.as_str(), sort))
                                .collect::<Vec<_>>(),
                            &frame_display,
                        )
                    });
                self.normalize_range_frame(&mut window_frame, &mut sorts, schema)?;
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
                let window = function(input)?;
                (name?, window)
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

        Ok(NamedExpr::new(vec![name], window))
    }

    fn resolve_offset_window_boundary(
        &self,
        function_name: &str,
        arguments: &[expr::Expr],
        schema: &DFSchemaRef,
    ) -> PlanResult<expr::Expr> {
        let mismatch = || {
            PlanError::invalid(format!(
                "[WINDOW_OFFSET_FRAME_MISMATCH] {function_name} frame must match its offset expression"
            ))
        };
        let offset = arguments
            .get(1)
            .cloned()
            .unwrap_or_else(|| datafusion_expr::lit(1_i32));
        if offset.is_volatile()
            || !offset.column_refs().is_empty()
            || offset.contains_outer()
            || offset.contains_scalar_subquery()
            || offset.exists(|expression| {
                Ok(match expression {
                    expr::Expr::ScalarFunction(function) => {
                        !function.func.inner().supports_constant_folding()
                    }
                    expr::Expr::HigherOrderFunction(function) => {
                        !function.func.supports_constant_folding()
                    }
                    _ => false,
                })
            })?
        {
            return Err(mismatch());
        }
        // Spark folds LAG's negated offset but compares frame expressions before
        // folding them. Numerically equal frame expressions can still be invalid.
        if function_name == "lag" {
            let context = SimplifyContextBuilder::default()
                .with_schema(schema.clone())
                .build();
            let negative =
                ScalarUDF::from(SparkNegative::new(self.config.ansi_mode)).call(vec![offset]);
            let coerced = ExprSimplifier::new(context).coerce(negative, schema)?;
            Ok(expr::Expr::Literal(
                LiteralEvaluator::new().evaluate(&coerced)?,
                None,
            ))
        } else {
            Ok(offset)
        }
    }

    /// Returns the function-required boundary after validating the supplied frame.
    async fn validate_offset_window_frame(
        &self,
        function_name: &str,
        arguments: &[expr::Expr],
        frame: &spec::WindowFrame,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<expr::Expr> {
        let mismatch = || {
            PlanError::invalid(format!(
                "[WINDOW_OFFSET_FRAME_MISMATCH] {function_name} frame must match its offset expression"
            ))
        };
        if frame.frame_type != spec::WindowFrameType::Row {
            return Err(mismatch());
        }
        let required = self.resolve_offset_window_boundary(function_name, arguments, schema)?;
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
        Ok(required)
    }

    async fn resolve_window_frame(
        &self,
        frame: spec::WindowFrame,
        order_by: &[expr::Sort],
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<(String, WindowFrame)> {
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
            return Ok((
                "RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING".into(),
                WindowFrame::new(None),
            ));
        }
        let ((lower_display, start), (upper_display, end)) = match units {
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
        if frame.has_null_offset() {
            if matches!(&frame.start_bound, WindowFrameBound::Following(value) if value.is_null())
                || matches!(&frame.end_bound, WindowFrameBound::Preceding(value) if value.is_null())
            {
                return Err(PlanError::invalid(
                    "[DATATYPE_MISMATCH.SPECIFIED_WINDOW_FRAME_INVALID_BOUND] invalid UNBOUNDED frame boundary",
                ));
            }
            let expression_type = |bound: &WindowFrameBound| match bound {
                WindowFrameBound::PrecedingNullOffset(data_type)
                | WindowFrameBound::FollowingNullOffset(data_type) => Some(data_type.clone()),
                WindowFrameBound::Preceding(value) | WindowFrameBound::Following(value)
                    if !value.is_null() =>
                {
                    Some(value.data_type())
                }
                _ => None,
            };
            if let (Some(start), Some(end)) = (
                expression_type(&frame.start_bound),
                expression_type(&frame.end_bound),
            ) {
                if start != end {
                    return Err(PlanError::invalid(
                        "[DATATYPE_MISMATCH.SPECIFIED_WINDOW_FRAME_DIFF_TYPES] expression frame bounds require the same type",
                    ));
                }
            }
        }
        if self.config.ansi_mode
            && let Some(order_key_type) = null_range_arithmetic_key_type(&frame, order_by, schema)?
            && let WindowFrameBound::Preceding(value) = &frame.start_bound
            && is_signed_integer_minimum(value)
        {
            // The left expression is evaluated before a NULL right endpoint short-circuits comparison.
            LiteralEvaluator::new().evaluate(
                &ScalarUDF::from(SparkRangeOffsetNegation::new(order_key_type))
                    .call(vec![datafusion_expr::lit(value.clone())]),
            )?;
        }
        let valid = if let Some((start, end)) = date_range_displacements(&frame, order_by, schema)?
        {
            // Spark compares expression endpoints; CURRENT ROW and UNBOUNDED skip this fold.
            if let (Some(start), Some(end)) = (start, end) {
                if self.config.ansi_mode {
                    for bound in [&frame.start_bound, &frame.end_bound] {
                        if matches!(
                            bound,
                            WindowFrameBound::Preceding(ScalarValue::Int32(Some(i32::MIN)))
                        ) {
                            LiteralEvaluator::new().evaluate(
                                &ScalarUDF::from(SparkRangeOffsetNegation::new(DataType::Date32))
                                    .call(vec![datafusion_expr::lit(i32::MIN)]),
                            )?;
                        }
                    }
                }
                start <= end
            } else {
                !matches!(&frame.start_bound, WindowFrameBound::Following(value) if value.is_null())
                    && !matches!(&frame.end_bound, WindowFrameBound::Preceding(value) if value.is_null())
            }
        } else if frame.has_null_offset() {
            true
        } else {
            datafusion::physical_planner::is_window_frame_bound_valid(&frame)
        };
        if !valid {
            return Err(PlanError::invalid(format!(
                "[DATATYPE_MISMATCH.SPECIFIED_WINDOW_FRAME_WRONG_COMPARISON] start bound ({}) cannot be larger than end bound ({})",
                frame.start_bound, frame.end_bound,
            )));
        }
        Ok((
            format!("{units} BETWEEN {lower_display} AND {upper_display}"),
            frame,
        ))
    }

    fn normalize_range_frame(
        &self,
        frame: &mut WindowFrame,
        order_by: &mut [expr::Sort],
        schema: &DFSchemaRef,
    ) -> PlanResult<()> {
        let spark_key_type = null_range_arithmetic_key_type(frame, order_by, schema)?;
        let date_displacements = date_range_displacements(frame, order_by, schema)?;
        if self.config.ansi_mode {
            let validation_key_type = if date_displacements.is_some() {
                Some(DataType::Date32)
            } else {
                spark_key_type.clone()
            };
            if let Some(order_key_type) = validation_key_type {
                let minimum_preceding = [&frame.start_bound, &frame.end_bound]
                    .into_iter()
                    .find_map(|bound| match bound {
                        WindowFrameBound::Preceding(value) if is_signed_integer_minimum(value) => {
                            Some(value.clone())
                        }
                        _ => None,
                    });
                if let Some(value) = minimum_preceding {
                    // Retain the key Field until the optimizer folds the deferred bound expression.
                    order_by[0].expr =
                        ScalarUDF::from(WindowKeyWithCheckedOffsetNegation::new(order_key_type))
                            .call(vec![order_by[0].expr.clone(), datafusion_expr::lit(value)]);
                } else if let Some((start, end)) = date_displacements
                    && !frame.has_null_offset()
                    && !order_by[0].asc
                    && [start, end].contains(&Some(i32::MIN))
                {
                    // DESC negation belongs to non-NULL key evaluation, including constant DATE keys.
                    order_by[0].expr = ScalarUDF::from(SparkDateDayArithmetic::new(
                        datafusion_expr::Operator::Plus,
                    ))
                    .call(vec![
                        order_by[0].expr.clone(),
                        ScalarUDF::from(SparkRangeOffsetNegation::new(DataType::Date32))
                            .call(vec![datafusion_expr::lit(i32::MIN)]),
                    ]);
                }
            }
        }
        if let Some((start, end)) = date_displacements {
            let start = match start {
                Some(days) if frame.end_bound == WindowFrameBound::CurrentRow => {
                    WindowFrameBound::Preceding(ScalarValue::Int32(Some(days.wrapping_neg())))
                }
                Some(days) => WindowFrameBound::Following(ScalarValue::Int32(Some(days))),
                None => frame.start_bound.clone(),
            };
            let end = match end {
                Some(days) => WindowFrameBound::Following(ScalarValue::Int32(Some(days))),
                None => frame.end_bound.clone(),
            };
            *frame = WindowFrame::new_bounds(frame.units, start, end);
        }
        if spark_key_type.is_some() {
            *frame = frame
                .clone()
                .with_range_offset_arithmetic(RangeOffsetArithmetic::Spark {
                    ansi_mode: self.config.ansi_mode,
                });
        }
        Ok(())
    }

    async fn resolve_window_boundary(
        &self,
        expr: spec::Expr,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<(String, ScalarValue)> {
        let (names, expressions) = self
            .resolve_expression_arguments(vec![expr], schema, state)
            .await?;
        let name = names.one()?;
        let resolved = expressions.one()?;
        if !resolved.column_refs().is_empty() || resolved.contains_outer() {
            return Err(PlanError::invalid(
                "[_LEGACY_ERROR_TEMP_0064] Frame bound value must be a literal.",
            ));
        }
        if let datafusion_expr::Expr::Literal(scalar, _) = resolved {
            return Ok((name, scalar));
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
        let value = evaluator.evaluate(&coerced).map_err(|e| {
            PlanError::invalid(format!(
                "window boundary must be a constant expression: {e}"
            ))
        })?;
        Ok((name, value))
    }

    async fn resolve_window_boundary_offset(
        &self,
        value: spec::WindowFrameBoundary,
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<(String, WindowFrameBound)> {
        let preceding = matches!(value, spec::WindowFrameBoundary::Preceding(_));
        match value {
            spec::WindowFrameBoundary::CurrentRow => {
                Ok(("CURRENT ROW".into(), WindowFrameBound::CurrentRow))
            }
            spec::WindowFrameBoundary::UnboundedPreceding => Ok((
                "UNBOUNDED PRECEDING".into(),
                WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
            )),
            spec::WindowFrameBoundary::UnboundedFollowing => Ok((
                "UNBOUNDED FOLLOWING".into(),
                WindowFrameBound::Following(ScalarValue::UInt64(None)),
            )),
            spec::WindowFrameBoundary::Preceding(expr)
            | spec::WindowFrameBoundary::Following(expr) => {
                let (display, value) = self.resolve_window_boundary(*expr, schema, state).await?;
                let ScalarValue::Int64(Some(offset)) = value.cast_to(&DataType::Int64)? else {
                    return Err(PlanError::invalid("invalid window boundary offset"));
                };
                let bound = if offset == 0 {
                    WindowFrameBound::CurrentRow
                } else if preceding == (offset > 0) {
                    WindowFrameBound::Preceding(ScalarValue::UInt64(Some(offset.unsigned_abs())))
                } else {
                    WindowFrameBound::Following(ScalarValue::UInt64(Some(offset.unsigned_abs())))
                };
                let direction = if preceding { "PRECEDING" } else { "FOLLOWING" };
                Ok((format!("{display} {direction}"), bound))
            }
            spec::WindowFrameBoundary::Value(expr) => {
                let (_, value) = self.resolve_window_boundary(*expr, schema, state).await?;
                let ScalarValue::Int64(Some(value)) = value.cast_to(&DataType::Int64)? else {
                    return Err(PlanError::invalid("invalid window boundary offset"));
                };
                let bound = match value {
                    i64::MIN => WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
                    i64::MAX => WindowFrameBound::Following(ScalarValue::UInt64(None)),
                    0 => WindowFrameBound::CurrentRow,
                    x if x < 0 => WindowFrameBound::Preceding(ScalarValue::UInt64(Some(-x as u64))),
                    x => WindowFrameBound::Following(ScalarValue::UInt64(Some(x as u64))),
                };
                Ok((bound.to_string(), bound))
            }
        }
    }

    async fn resolve_window_boundary_value(
        &self,
        value: spec::WindowFrameBoundary,
        order_by: &[expr::Sort],
        schema: &DFSchemaRef,
        state: &mut PlanResolverState,
    ) -> PlanResult<(String, WindowFrameBound)> {
        // Helper to get the data type from the order_by expression for casting
        let get_order_by_type = || -> PlanResult<DataType> {
            let [order_by] = order_by else {
                return Err(PlanError::invalid(
                    "range window frame requires exactly one order by expression",
                ));
            };
            let field = order_by.expr.to_field(schema)?.1;
            let mut data_type = field.data_type();
            while let DataType::Dictionary(_, value_type) = data_type {
                data_type = value_type;
            }
            // The late window rewrite decodes scalar dictionary floats to numeric keys.
            if matches!(data_type, DataType::Float32 | DataType::Float64) {
                return Ok(data_type.clone());
            }
            Ok(field.data_type().clone())
        };

        let preceding = matches!(value, spec::WindowFrameBoundary::Preceding(_));
        match value {
            spec::WindowFrameBoundary::CurrentRow => {
                Ok(("CURRENT ROW".into(), WindowFrameBound::CurrentRow))
            }
            spec::WindowFrameBoundary::UnboundedPreceding => Ok((
                "UNBOUNDED PRECEDING".into(),
                WindowFrameBound::Preceding(ScalarValue::Null),
            )),
            spec::WindowFrameBoundary::UnboundedFollowing => Ok((
                "UNBOUNDED FOLLOWING".into(),
                WindowFrameBound::Following(ScalarValue::Null),
            )),
            spec::WindowFrameBoundary::Preceding(expr)
            | spec::WindowFrameBoundary::Following(expr) => {
                let (display, value) = self.resolve_window_boundary(*expr, schema, state).await?;
                // Spark gives unary minus on an untyped NULL a DOUBLE operand.
                let value = if preceding && matches!(value, ScalarValue::Null) {
                    ScalarValue::Float64(None)
                } else {
                    value
                };
                if value.is_null() && order_by.len() > 1 {
                    return Err(PlanError::invalid(
                        "[DATATYPE_MISMATCH.RANGE_FRAME_MULTI_ORDER] range window frame requires exactly one order by expression",
                    ));
                }
                let data_type = get_order_by_type()?;
                if value.is_null() {
                    let source_type = value.data_type();
                    let coerced_type =
                        if !matches!(data_type, DataType::Date32 | DataType::Timestamp(_, _))
                            && can_cast_types(&source_type, &data_type)
                        {
                            &data_type
                        } else {
                            &source_type
                        };
                    if !coerced_type.is_numeric()
                        && !matches!(coerced_type, DataType::Interval(_) | DataType::Duration(_))
                    {
                        return Err(PlanError::invalid(
                            "[DATATYPE_MISMATCH.SPECIFIED_WINDOW_FRAME_UNACCEPTED_TYPE] RANGE expression bounds require a numeric or interval type",
                        ));
                    }
                    if !data_type.is_numeric()
                        && !matches!(data_type, DataType::Date32 | DataType::Timestamp(_, _))
                    {
                        return Err(PlanError::invalid(
                            "[DATATYPE_MISMATCH.RANGE_FRAME_INVALID_TYPE] unsupported RANGE order key and offset type",
                        ));
                    }
                }
                if data_type == DataType::Date32
                    && !matches!(
                        value.data_type(),
                        DataType::Int32
                            | DataType::Interval(_)
                            | DataType::Duration(_)
                            | DataType::Null
                    )
                {
                    return Err(PlanError::invalid(
                        "[DATATYPE_MISMATCH.RANGE_FRAME_INVALID_TYPE] numeric DATE RANGE bounds require INT days",
                    ));
                }
                // Temporal offsets retain their admitted Interval/Duration representation.
                let value = if data_type.is_numeric() {
                    value.cast_to(&data_type)?
                } else {
                    value
                };
                let (direction, bound) = if value.is_null()
                    && (data_type.is_numeric()
                        || (data_type == DataType::Date32 && value.data_type() == DataType::Int32))
                {
                    if preceding {
                        (
                            "PRECEDING",
                            WindowFrameBound::PrecedingNullOffset(value.data_type()),
                        )
                    } else {
                        (
                            "FOLLOWING",
                            WindowFrameBound::FollowingNullOffset(value.data_type()),
                        )
                    }
                } else if preceding {
                    ("PRECEDING", WindowFrameBound::Preceding(value))
                } else {
                    ("FOLLOWING", WindowFrameBound::Following(value))
                };
                Ok((format!("{display} {direction}"), bound))
            }
            spec::WindowFrameBoundary::Value(expr) => {
                let (_, value) = self.resolve_window_boundary(*expr, schema, state).await?;
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
                    let bound = match value.partial_cmp(&zero) {
                        None => {
                            return Err(PlanError::invalid(
                                "cannot compare window boundary value with zero",
                            ));
                        }
                        Some(Ordering::Less) => {
                            let value = value.arithmetic_negate()?;
                            WindowFrameBound::Preceding(value)
                        }
                        Some(Ordering::Greater) => WindowFrameBound::Following(value),
                        Some(Ordering::Equal) => WindowFrameBound::CurrentRow,
                    };
                    Ok((bound.to_string(), bound))
                }
            }
        }
    }
}

fn null_range_arithmetic_key_type(
    frame: &WindowFrame,
    order_by: &[expr::Sort],
    schema: &DFSchemaRef,
) -> PlanResult<Option<DataType>> {
    if frame.units != WindowFrameUnits::Range || !frame.has_null_offset() {
        return Ok(None);
    }
    let [order] = order_by else {
        return Ok(None);
    };
    let key_type = order.expr.get_type(schema)?;
    Ok(matches!(
        key_type,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 | DataType::Date32
    )
    .then_some(key_type))
}

fn is_signed_integer_minimum(value: &ScalarValue) -> bool {
    matches!(
        value,
        ScalarValue::Int8(Some(i8::MIN))
            | ScalarValue::Int16(Some(i16::MIN))
            | ScalarValue::Int32(Some(i32::MIN))
            | ScalarValue::Int64(Some(i64::MIN))
    )
}

// Special boundaries remain distinct from an expression whose signed displacement is zero.
fn date_range_displacements(
    frame: &WindowFrame,
    order_by: &[expr::Sort],
    schema: &DFSchemaRef,
) -> PlanResult<Option<(Option<i32>, Option<i32>)>> {
    let [order] = order_by else {
        return Ok(None);
    };
    if frame.units != WindowFrameUnits::Range || order.expr.get_type(schema)? != DataType::Date32 {
        return Ok(None);
    }
    let displacement = |bound: &WindowFrameBound| match bound {
        WindowFrameBound::Preceding(ScalarValue::Int32(Some(days))) => {
            Some(Some(days.wrapping_neg()))
        }
        WindowFrameBound::Following(ScalarValue::Int32(Some(days))) => Some(Some(*days)),
        WindowFrameBound::CurrentRow => Some(None),
        WindowFrameBound::PrecedingNullOffset(DataType::Int32)
        | WindowFrameBound::FollowingNullOffset(DataType::Int32) => Some(None),
        WindowFrameBound::Preceding(value) | WindowFrameBound::Following(value)
            if value.is_null() =>
        {
            Some(None)
        }
        _ => None,
    };
    Ok(displacement(&frame.start_bound).zip(displacement(&frame.end_bound)))
}

// Delegate negation, adding Spark's condition only to this offset type's exact overflow cause.
#[derive(Debug, PartialEq, Eq, Hash)]
struct SparkRangeOffsetNegation {
    order_key_type: DataType,
    signature: Signature,
}

impl SparkRangeOffsetNegation {
    fn new(order_key_type: DataType) -> Self {
        let offset_type = if order_key_type == DataType::Date32 {
            DataType::Int32
        } else {
            order_key_type.clone()
        };
        Self {
            order_key_type,
            signature: Signature::exact(vec![offset_type], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkRangeOffsetNegation {
    fn name(&self) -> &str {
        if self.order_key_type == DataType::Date32 {
            "spark_date_range_negation"
        } else {
            "spark_range_offset_negation"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, types: &[DataType]) -> datafusion_common::Result<DataType> {
        SparkNegative::new(true).return_type(types)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> datafusion_common::Result<FieldRef> {
        SparkNegative::new(true).return_field_from_args(args)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        SparkNegative::new(true)
            .invoke_with_args(args)
            .map_err(|error| {
                let expected_cause = match self.order_key_type {
                    DataType::Int8 => "Int8 overflow on negative(-128)",
                    DataType::Int16 => "Int16 overflow on negative(-32768)",
                    DataType::Int32 | DataType::Date32 => {
                        "Int32 overflow on negative(-2147483648)"
                    }
                    DataType::Int64 => "Int64 overflow on negative(-9223372036854775808)",
                    _ => return error,
                };
                if matches!(&error, DataFusionError::Execution(message) if message == expected_cause) {
                    error.context(if self.order_key_type == DataType::Date32 {
                        "[ARITHMETIC_OVERFLOW] DATE RANGE offset negation"
                    } else {
                        "[ARITHMETIC_OVERFLOW] RANGE offset negation"
                    })
                } else {
                    error
                }
            })
    }
}

// Retain the original key declaration while a PRECEDING expression awaits constant folding.
#[derive(Debug, PartialEq, Eq, Hash)]
struct WindowKeyWithCheckedOffsetNegation {
    order_key_type: DataType,
    signature: Signature,
}

impl WindowKeyWithCheckedOffsetNegation {
    fn new(order_key_type: DataType) -> Self {
        let offset_type = if order_key_type == DataType::Date32 {
            DataType::Int32
        } else {
            order_key_type.clone()
        };
        Self {
            signature: Signature::exact(
                vec![order_key_type.clone(), offset_type],
                Volatility::Immutable,
            ),
            order_key_type,
        }
    }
}

impl ScalarUDFImpl for WindowKeyWithCheckedOffsetNegation {
    fn name(&self) -> &str {
        if self.order_key_type == DataType::Date32 {
            "spark_date_range_bound_check"
        } else {
            "window_key_with_checked_offset_negation"
        }
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(self.order_key_type.clone())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> datafusion_common::Result<FieldRef> {
        Ok(args.arg_fields[0].clone())
    }

    fn simplify(
        &self,
        args: Vec<expr::Expr>,
        _: &datafusion_expr::simplify::SimplifyContext,
    ) -> datafusion_common::Result<datafusion_expr::simplify::ExprSimplifyResult> {
        let [key, bound] = datafusion_common::utils::take_function_args(self.name(), args)?;
        LiteralEvaluator::new().evaluate(
            &ScalarUDF::from(SparkRangeOffsetNegation::new(self.order_key_type.clone()))
                .call(vec![bound]),
        )?;
        Ok(datafusion_expr::simplify::ExprSimplifyResult::Simplified(
            key,
        ))
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let [key, bound] =
            datafusion_common::utils::take_function_args(self.name(), args.args.clone())?;
        SparkRangeOffsetNegation::new(self.order_key_type.clone()).invoke_with_args(
            ScalarFunctionArgs {
                args: vec![bound],
                arg_fields: vec![args.arg_fields[1].clone()],
                return_field: args.arg_fields[1].clone(),
                ..args
            },
        )?;
        Ok(key)
    }
}

#[cfg(test)]
mod range_frame_tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::physical_expr::create_physical_expr;
    use datafusion_common::DFSchema;
    use datafusion_expr::execution_props::ExecutionProps;
    use datafusion_expr::{Operator, col, lit};

    use super::*;

    #[test]
    fn date_range_negation_keeps_declarations_and_spark_failure_phases()
    -> datafusion_common::Result<()> {
        fn error_message<T>(
            result: datafusion_common::Result<T>,
        ) -> datafusion_common::Result<String> {
            result
                .err()
                .map(|error| error.to_string())
                .ok_or_else(|| DataFusionError::Internal("expected DATE RANGE failure".into()))
        }
        let key_field = Arc::new(
            Field::new("day", DataType::Date32, true)
                .with_metadata(HashMap::from([("source".into(), "kept".into())])),
        );
        let offset_field = Arc::new(Field::new("days", DataType::Int32, false));
        let schema = Arc::new(DFSchema::try_from(Schema::new(vec![key_field.clone()]))?);
        let fields = [key_field.clone(), offset_field];
        let minimum = ScalarValue::Int32(Some(i32::MIN));
        assert_eq!(
            SparkRangeOffsetNegation::new(DataType::Date32).name(),
            "spark_date_range_negation"
        );
        assert_eq!(
            SparkRangeOffsetNegation::new(DataType::Date32).signature(),
            &Signature::exact(vec![DataType::Int32], Volatility::Immutable)
        );
        assert_eq!(
            WindowKeyWithCheckedOffsetNegation::new(DataType::Date32).name(),
            "spark_date_range_bound_check"
        );
        let actual = WindowKeyWithCheckedOffsetNegation::new(DataType::Date32)
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &fields,
                scalar_arguments: &[None, Some(&minimum)],
            })?;
        assert_eq!(actual, key_field);
        let simplifier = ExprSimplifier::new(
            SimplifyContextBuilder::default()
                .with_schema(schema.clone())
                .build(),
        );
        for key in [
            col("day"),
            lit(ScalarValue::Date32(None)),
            lit(ScalarValue::Date32(Some(0))),
        ] {
            let checked =
                ScalarUDF::from(WindowKeyWithCheckedOffsetNegation::new(DataType::Date32))
                    .call(vec![key, lit(i32::MIN)]);
            let error = error_message(simplifier.simplify(checked))?;
            assert!(error.contains("[ARITHMETIC_OVERFLOW]"), "{error}");
            assert!(
                error.contains("Int32 overflow on negative(-2147483648)"),
                "{error}"
            );
        }
        let arguments = |value: ScalarValue| {
            let field = Arc::new(Field::new("operand", value.data_type(), value.is_null()));
            ScalarFunctionArgs {
                args: vec![ColumnarValue::Scalar(value)],
                arg_fields: vec![field.clone()],
                number_rows: 1,
                return_field: field,
                config_options: Arc::default(),
            }
        };
        let error = error_message(
            SparkRangeOffsetNegation::new(DataType::Date32)
                .invoke_with_args(arguments(ScalarValue::Int64(Some(i64::MIN)))),
        )?;
        assert!(!error.contains("[ARITHMETIC_OVERFLOW]"), "{error}");
        let mut args = arguments(minimum);
        args.args
            .insert(0, ColumnarValue::Scalar(ScalarValue::Date32(Some(0))));
        args.arg_fields.insert(0, key_field.clone());
        args.return_field = key_field;
        assert!(
            error_message(
                WindowKeyWithCheckedOffsetNegation::new(DataType::Date32).invoke_with_args(args)
            )?
            .contains("[ARITHMETIC_OVERFLOW]")
        );

        let desc_key = |key| {
            ScalarUDF::from(SparkDateDayArithmetic::new(Operator::Plus)).call(vec![
                key,
                ScalarUDF::from(SparkRangeOffsetNegation::new(DataType::Date32))
                    .call(vec![lit(i32::MIN)]),
            ])
        };
        let null_key = simplifier.simplify(desc_key(lit(ScalarValue::Date32(None))))?;
        assert_eq!(
            LiteralEvaluator::new().evaluate(&null_key)?,
            ScalarValue::Date32(None)
        );
        let non_null_key = simplifier.simplify(desc_key(lit(ScalarValue::Date32(Some(0)))))?;
        assert!(
            error_message(LiteralEvaluator::new().evaluate(&non_null_key))?
                .contains("[ARITHMETIC_OVERFLOW]")
        );
        let physical =
            create_physical_expr(&desc_key(col("day")), &schema, &ExecutionProps::new())?;
        let empty = RecordBatch::new_empty(schema.inner().clone());
        assert_eq!(physical.evaluate(&empty)?.into_array(0)?.len(), 0);
        Ok(())
    }

    #[test]
    fn integer_range_negation_checks_each_width_and_keeps_the_key_field()
    -> datafusion_common::Result<()> {
        for minimum in [
            ScalarValue::Int8(Some(i8::MIN)),
            ScalarValue::Int16(Some(i16::MIN)),
            ScalarValue::Int32(Some(i32::MIN)),
            ScalarValue::Int64(Some(i64::MIN)),
        ] {
            let key_type = minimum.data_type();
            let zero = ScalarValue::new_zero(&key_type)?;
            assert!(is_signed_integer_minimum(&minimum));
            assert!(!is_signed_integer_minimum(&zero));
            for nullable in [false, true] {
                let key_field = Arc::new(
                    Field::new("original_key", key_type.clone(), nullable)
                        .with_metadata(HashMap::from([("source".into(), "kept".into())])),
                );
                let offset_field = Arc::new(Field::new("offset", key_type.clone(), false));
                let fields = [key_field.clone(), offset_field.clone()];
                let guard = WindowKeyWithCheckedOffsetNegation::new(key_type.clone());
                assert_eq!(
                    guard.return_field_from_args(ReturnFieldArgs {
                        arg_fields: &fields,
                        scalar_arguments: &[None, Some(&minimum)],
                    })?,
                    key_field
                );
                assert_eq!(
                    guard.signature(),
                    &Signature::exact(
                        vec![key_type.clone(), key_type.clone()],
                        Volatility::Immutable
                    )
                );
                let schema = Arc::new(DFSchema::try_from(Schema::new(vec![key_field.clone()]))?);
                let simplifier = ExprSimplifier::new(
                    SimplifyContextBuilder::default()
                        .with_schema(schema)
                        .build(),
                );
                for key in [
                    col("original_key"),
                    lit(ScalarValue::try_from(&key_type)?),
                    lit(zero.clone()),
                ] {
                    let checked =
                        ScalarUDF::from(WindowKeyWithCheckedOffsetNegation::new(key_type.clone()))
                            .call(vec![key, lit(minimum.clone())]);
                    let error = simplifier
                        .simplify(checked)
                        .expect_err("minimum offset must fail");
                    let message = error.to_string();
                    assert!(
                        message.contains("[ARITHMETIC_OVERFLOW] RANGE offset negation"),
                        "{message}"
                    );
                    assert!(
                        message.contains(&format!("{key_type:?} overflow on negative(")),
                        "{message}"
                    );
                }
                let key_value = ScalarValue::try_from(&key_type)?;
                let checked = guard.invoke_with_args(ScalarFunctionArgs {
                    args: vec![
                        ColumnarValue::Scalar(key_value.clone()),
                        ColumnarValue::Scalar(zero.clone()),
                    ],
                    arg_fields: fields.to_vec(),
                    number_rows: 1,
                    return_field: key_field,
                    config_options: Arc::default(),
                })?;
                assert!(matches!(checked, ColumnarValue::Scalar(value) if value == key_value));
            }
        }
        Ok(())
    }
}
