// Modified from Sail v0.7.1 for the Delta reader experiment. See crates/delta-arrow-reader-spark-sql/UPSTREAM.md in the host repository.
use std::{borrow::Cow, sync::Arc};

use async_trait::async_trait;
use datafusion::execution::SessionState;
use datafusion::execution::context::QueryPlanner;
use datafusion::physical_expr::{LexOrdering, OrderingRequirements, create_physical_sort_exprs};
use datafusion::physical_optimizer::output_requirements::OutputRequirementExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{DataFusionError, Result, internal_err};
use datafusion_expr::{LogicalPlan, UserDefinedLogicalNode};
use sail_logical_plan::monotonic_id::MonotonicIdNode;
use sail_logical_plan::sort::{RequiredSortNode, SortWithinPartitionsNode};
use sail_logical_plan::spark_partition_id::SparkPartitionIdNode;

use self::input_sort_boundary::InputSortBoundaryExec;
use self::monotonic_id::MonotonicIdExec;
use self::signed_range_window::{
    SparkSignedRangeWindowNode, lower_signed_range_windows, plan_signed_range_window,
};
use self::spark_partition_id::SparkPartitionIdExec;
use crate::expression_errors::SparkSqlExpressionSemantics;
use crate::function::scalar::{apply_unpivot_runtime_nullability, lower_runtime_concat};
use crate::projection_row_errors::{ProjectionRowErrorNode, admit_prefix_producers};

mod input_sort_boundary;
mod monotonic_id;
mod signed_range_window;
mod spark_partition_id;

/// Plan native DataFusion queries and the supported Spark execution extensions.
#[derive(Debug)]
pub struct SparkQueryPlanner;

#[async_trait]
impl QueryPlanner for SparkQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut state = Cow::Borrowed(session_state);
        logical_plan.apply_with_subqueries(|plan| {
            if let LogicalPlan::Extension(extension) = plan
                && let Some(sort) = extension.node.as_any().downcast_ref::<RequiredSortNode>()
                && !sort.preserve_partitioning()
            {
                // ponytail: this plan disables automatic round-robin repartitioning to
                // keep the required global order. Restore parallelism when aggregate
                // ordering keys can be carried through projections that hide them.
                state
                    .to_mut()
                    .config_mut()
                    .options_mut()
                    .optimizer
                    .enable_round_robin_repartition = false;
                return Ok(TreeNodeRecursion::Stop);
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        let runtime_plan = if session_state
            .config_options()
            .extensions
            .get::<SparkSqlExpressionSemantics>()
            .is_some()
        {
            Cow::Owned(lower_signed_range_windows(admit_prefix_producers(
                logical_plan
                    .clone()
                    .transform_up_with_subqueries(|node| {
                        let names = datafusion_expr::expr_rewriter::NamePreserver::new(&node);
                        node.map_expressions(|expression| {
                            let name = names.save(&expression);
                            expression
                                .transform_up(|expression| {
                                    lower_runtime_concat(expression)
                                        .map_err(|error| {
                                            DataFusionError::External(Box::new(error))
                                        })?
                                        .transform_data(apply_unpivot_runtime_nullability)
                                })
                                .map(|expression| {
                                    expression.update_data(|expression| name.restore(expression))
                                })
                        })
                    })?
                    .data,
            )?)?)
        } else {
            Cow::Borrowed(logical_plan)
        };
        DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(SparkExtensionPlanner)])
            .create_physical_plan(&runtime_plan, &state)
            .await
    }
}

// Keep callers on SparkQueryPlanner so required-order safeguards also apply.
struct SparkExtensionPlanner;

#[async_trait]
impl ExtensionPlanner for SparkExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let plan: Arc<dyn ExecutionPlan> = if let Some(window) =
            node.as_any().downcast_ref::<SparkSignedRangeWindowNode>()
        {
            let [input] = physical_inputs else {
                return internal_err!("SparkSignedRangeWindow requires exactly one physical input");
            };
            plan_signed_range_window(window, Arc::clone(input), session_state)?
        } else if let Some(provenance) = node.as_any().downcast_ref::<ProjectionRowErrorNode>() {
            let [input] = physical_inputs else {
                return internal_err!(
                    "Projection row-error provenance requires exactly one physical input"
                );
            };
            match provenance {
                ProjectionRowErrorNode::InputSortBoundary(_) => {
                    if let Some(sort) = input.downcast_ref::<SortExec>() {
                        Arc::new(InputSortBoundaryExec::new(sort.clone()))
                    } else {
                        // shortcut: protect retained sorts; restore removed sorts when their live specification is retained.
                        Arc::clone(input)
                    }
                }
                ProjectionRowErrorNode::PrefixProducer(_) => {
                    let Some(projection) = input.downcast_ref::<ProjectionExec>() else {
                        return internal_err!(
                            "A row-error prefix producer must be a computed projection"
                        );
                    };
                    if projection.schema().fields() != node.schema().fields() {
                        return internal_err!(
                            "A row-error prefix producer changed its bound output fields"
                        );
                    }
                    Arc::new(projection.clone().with_row_error_prefix(true))
                }
            }
        } else if let Some(partition_id) = node.as_any().downcast_ref::<SparkPartitionIdNode>() {
            let [input] = physical_inputs else {
                return internal_err!("SparkPartitionIdExec requires exactly one physical input");
            };
            Arc::new(SparkPartitionIdExec::try_new(
                input.clone(),
                partition_id.column_name(),
                node.schema().inner().clone(),
            )?)
        } else if let Some(monotonic_id) = node.as_any().downcast_ref::<MonotonicIdNode>() {
            let [input] = physical_inputs else {
                return internal_err!("MonotonicIdExec requires exactly one physical input");
            };
            Arc::new(MonotonicIdExec::try_new(
                input.clone(),
                monotonic_id.column_name(),
                node.schema().inner().clone(),
            )?)
        } else if let Some(sort) = node.as_any().downcast_ref::<SortWithinPartitionsNode>() {
            let [input] = physical_inputs else {
                return internal_err!("SortExec requires exactly one physical input");
            };
            let expr = create_physical_sort_exprs(
                sort.sort_expr(),
                node.schema(),
                session_state.execution_props(),
            )?;
            let Some(ordering) = LexOrdering::new(expr) else {
                return internal_err!("SortExec requires at least one sort expression");
            };
            Arc::new(
                SortExec::new(ordering, input.clone())
                    .with_fetch(sort.fetch())
                    .with_preserve_partitioning(true),
            )
        } else if let Some(sort) = node.as_any().downcast_ref::<RequiredSortNode>() {
            let [input] = physical_inputs else {
                return internal_err!("RequiredSort requires exactly one physical input");
            };
            let expr = create_physical_sort_exprs(
                sort.sort_expr(),
                node.schema(),
                session_state.execution_props(),
            )?;
            let Some(ordering) = LexOrdering::new(expr) else {
                return internal_err!("RequiredSort requires at least one sort expression");
            };
            let physical = SortExec::new(ordering, input.clone())
                .with_fetch(sort.fetch())
                .with_preserve_partitioning(sort.preserve_partitioning());
            let requirements = OrderingRequirements::from(physical.expr().clone());
            let distribution = physical.required_input_distribution().swap_remove(0);
            Arc::new(OutputRequirementExec::new(
                Arc::new(physical),
                Some(requirements),
                distribution,
                sort.fetch(),
            ))
        } else {
            return Ok(None);
        };
        Ok(Some(plan))
    }
}
