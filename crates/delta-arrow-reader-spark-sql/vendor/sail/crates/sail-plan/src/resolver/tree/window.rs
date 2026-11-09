// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::collections::HashMap;
use std::mem;
use std::sync::Arc;

use datafusion::common::Result;
use datafusion::functions_window::nth_value::{NthValue, NthValueKind};
use datafusion::logical_expr::logical_plan::Window;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter};
use datafusion_common::{DFSchemaRef, DataFusionError, internal_datafusion_err};
use datafusion_expr::expr::{WindowFunction, WindowFunctionDefinition};
use datafusion_expr::{
    Expr, Extension, LogicalPlan, Projection, SortExpr, WindowFrame, WindowFrameUnits, ident,
};
use sail_logical_plan::monotonic_id::{MonotonicIdNode, MonotonicIdPurpose};
use sail_logical_plan::sort::RequiredSortNode;

use crate::error::PlanResult;
use crate::function::sort_keys::spark_sort_key;
use crate::resolver::PlanResolver;
use crate::resolver::state::PlanResolverState;
use crate::resolver::tree::{
    PlanRewriter, contains_seeded_rand, empty_logical_plan,
    skip_aggregate_arguments_needing_validation,
};

pub(crate) struct WindowRewriter<'s> {
    plan: LogicalPlan,
    state: &'s mut PlanResolverState,
    /// Cache of already-processed window functions keyed by schema name.
    /// This deduplicates identical window function expressions that appear
    /// multiple times in the expression tree (e.g. when a division-by-zero
    /// guard wraps a window-function divisor in a CASE expression).
    seen: HashMap<String, Expr>,
    input_order_initialized: bool,
    stable_ordering: Option<Vec<SortExpr>>,
    stable_ordering_enforced: bool,
    input_before_windows: Option<LogicalPlan>,
    rand_projection: Option<Projection>,
}

impl WindowRewriter<'_> {
    fn materialize_rand_inputs(&mut self, function: &mut WindowFunction) -> Result<bool> {
        let input = self
            .input_before_windows
            .as_ref()
            .ok_or_else(|| internal_datafusion_err!("Window input was not initialized"))?;
        let mut projected = match &self.rand_projection {
            Some(projection) => projection.expr.clone(),
            None => input.schema().columns().into_iter().map(Expr::Column).collect(),
        };
        let previous_width = projected.len();
        let mut materialize = |expression: &mut Expr| -> Result<()> {
            if contains_seeded_rand(expression)? {
                let field = self.state.register_hidden_field_name("__rand_window_input");
                projected.push(expression.clone().alias(&field));
                *expression = ident(field);
            }
            Ok(())
        };
        for argument in &mut function.params.args {
            materialize(argument)?;
        }
        for partition in &mut function.params.partition_by {
            materialize(partition)?;
        }
        for order in &mut function.params.order_by {
            materialize(&mut order.expr)?;
        }
        let materialized = projected.len() > previous_width;
        if materialized {
            let previous = self
                .rand_projection
                .as_ref()
                .map_or_else(|| input.clone(), |projection| LogicalPlan::Projection(projection.clone()));
            let projection = Projection::try_new(projected, Arc::new(input.clone()))?;
            let replacement = LogicalPlan::Projection(projection.clone());
            let plan = mem::replace(&mut self.plan, empty_logical_plan());
            // Stop at the common input: only windows and sorts added by this
            // rewriter need wider schemas. Original input boundaries stay intact.
            self.plan = plan
                .transform_down_up(
                    |node| {
                        if node == previous {
                            Ok(Transformed::new(
                                replacement.clone(),
                                true,
                                TreeNodeRecursion::Jump,
                            ))
                        } else {
                            Ok(Transformed::no(node))
                        }
                    },
                    |node| node.recompute_schema().map(Transformed::yes),
                )?
                .data;
            self.rand_projection = Some(projection);
        }
        Ok(materialized)
    }

    fn requires_input_order(function: &WindowFunction, schema: &DFSchemaRef) -> PlanResult<bool> {
        let function_requires_order = match &function.fun {
            WindowFunctionDefinition::AggregateUDF(udaf) => {
                !PlanResolver::is_order_irrelevant_udaf(
                    udaf.as_ref(),
                    &function.params.args,
                    schema,
                )?
            }
            WindowFunctionDefinition::WindowUDF(udwf) => udwf
                .inner()
                .downcast_ref::<NthValue>()
                .is_some_and(|nth| matches!(nth.kind(), NthValueKind::First | NthValueKind::Last)),
        };
        let frame_requires_order =
            matches!(&function.fun, WindowFunctionDefinition::AggregateUDF(_))
                && function.params.window_frame.units == WindowFrameUnits::Rows
                && function.params.window_frame != WindowFrame::new(None);
        Ok(function_requires_order || frame_requires_order)
    }
}

impl<'s> PlanRewriter<'s> for WindowRewriter<'s> {
    fn new_from_plan(plan: LogicalPlan, state: &'s mut PlanResolverState) -> Self {
        Self {
            plan,
            state,
            seen: HashMap::new(),
            input_order_initialized: false,
            stable_ordering: None,
            stable_ordering_enforced: false,
            input_before_windows: None,
            rand_projection: None,
        }
    }

    fn into_plan(self) -> LogicalPlan {
        self.plan
    }
}

impl TreeNodeRewriter for WindowRewriter<'_> {
    type Node = Expr;

    fn f_down(&mut self, node: Expr) -> Result<Transformed<Expr>> {
        skip_aggregate_arguments_needing_validation(node)
    }

    fn f_up(&mut self, node: Expr) -> Result<Transformed<Expr>> {
        match node {
            Expr::WindowFunction(mut function) => {
                // Grouping placeholders are rebased before comparison-key typing.
                for partition in &mut function.params.partition_by {
                    *partition = spark_sort_key(partition.clone(), self.plan.schema())?;
                }
                for sort in &mut function.params.order_by {
                    sort.expr = spark_sort_key(sort.expr.clone(), self.plan.schema())?;
                }
                let original = Expr::WindowFunction(function.clone());
                let name = original.schema_name().to_string();
                let seeded_rand = contains_seeded_rand(&original)?;
                if seeded_rand
                    && let Some(replacement) = self.seen.get(&name)
                {
                    return Ok(Transformed::yes(replacement.clone()));
                }
                let requires_input_order =
                    Self::requires_input_order(&function, self.plan.schema())
                        .map_err(|error| DataFusionError::External(Box::new(error)))?;

                if !self.input_order_initialized {
                    self.input_order_initialized = true;
                    let (plan, found, global) =
                        PlanResolver::require_input_sort_inner(self.plan.clone())
                            .map_err(|error| DataFusionError::External(Box::new(error)))?;
                    self.plan = plan;
                    if found && global {
                        let mut ordering =
                            PlanResolver::input_sort_ordering(&self.plan).unwrap_or_default();
                        let column = self.state.register_hidden_field_name("");
                        self.plan = LogicalPlan::Extension(Extension {
                            node: Arc::new(MonotonicIdNode::try_new(
                                Arc::new(self.plan.clone()),
                                column.clone(),
                                MonotonicIdPurpose::WindowOrdering,
                            )?),
                        });
                        ordering.push(SortExpr {
                            expr: ident(column),
                            asc: true,
                            nulls_first: true,
                        });
                        self.stable_ordering = Some(ordering);
                        self.stable_ordering_enforced = true;
                    }
                }

                if self.input_before_windows.is_none() {
                    self.input_before_windows = Some(self.plan.clone());
                }
                let materialized_rand = seeded_rand && self.materialize_rand_inputs(&mut function)?;
                let mut required = function
                    .params
                    .partition_by
                    .iter()
                    .cloned()
                    .map(|expr| SortExpr {
                        expr,
                        asc: true,
                        nulls_first: true,
                    })
                    .collect::<Vec<_>>();
                required.extend(function.params.order_by.clone());

                if let Some(prior) = self.stable_ordering.clone() {
                    let prior_satisfies_required =
                        required.is_empty() || prior.starts_with(&required);
                    let already_enforced =
                        self.stable_ordering_enforced && prior_satisfies_required;
                    let desired = if prior_satisfies_required {
                        prior
                    } else {
                        for sort in prior {
                            if !required.iter().any(|x| x.expr == sort.expr) {
                                required.push(sort);
                            }
                        }
                        required
                    };

                    if requires_input_order && !already_enforced {
                        self.plan = LogicalPlan::Extension(Extension {
                            node: Arc::new(RequiredSortNode::new(
                                Arc::new(self.plan.clone()),
                                desired.clone(),
                                None,
                                false,
                            )),
                        });
                        self.stable_ordering_enforced = true;
                    } else if !already_enforced {
                        self.stable_ordering_enforced = false;
                    }
                    self.stable_ordering = Some(desired);
                }
                let node = Expr::WindowFunction(function);
                let node = if materialized_rand { node.alias(&name) } else { node };
                if let Some(replacement) = self.seen.get(&name) {
                    return Ok(Transformed::yes(replacement.clone()));
                }
                let plan = mem::replace(&mut self.plan, empty_logical_plan());
                self.plan = LogicalPlan::Window(Window::try_new(vec![node], Arc::new(plan))?);
                let alias = self.state.register_field_name(&name);
                let replacement = ident(name.clone()).alias(alias);
                self.seen.insert(name, replacement.clone());
                Ok(Transformed::yes(replacement))
            }
            _ => Ok(Transformed::no(node)),
        }
    }
}
