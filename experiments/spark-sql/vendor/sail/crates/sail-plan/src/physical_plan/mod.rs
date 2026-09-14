// Modified from Sail v0.7.1 for the Delta reader experiment. See experiments/spark-sql/UPSTREAM.md in the host repository.
use std::{borrow::Cow, sync::Arc};

use async_trait::async_trait;
use datafusion::execution::SessionState;
use datafusion::execution::context::QueryPlanner;
use datafusion::physical_expr::{LexOrdering, OrderingRequirements, create_physical_sort_exprs};
use datafusion::physical_optimizer::output_requirements::OutputRequirementExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{Result, internal_err};
use datafusion_expr::{LogicalPlan, UserDefinedLogicalNode};
use sail_logical_plan::sort::{RequiredSortNode, SortWithinPartitionsNode};
use sail_logical_plan::spark_partition_id::SparkPartitionIdNode;

use self::spark_partition_id::SparkPartitionIdExec;

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
        DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(SparkExtensionPlanner)])
            .create_physical_plan(logical_plan, &state)
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
        let plan: Arc<dyn ExecutionPlan> = if let Some(partition_id) =
            node.as_any().downcast_ref::<SparkPartitionIdNode>()
        {
            let [input] = physical_inputs else {
                return internal_err!("SparkPartitionIdExec requires exactly one physical input");
            };
            Arc::new(SparkPartitionIdExec::try_new(
                input.clone(),
                partition_id.column_name(),
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
