// Modified from Sail v0.7.1 for the Delta reader experiment. See experiments/spark-sql/UPSTREAM.md in the host repository.
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::execution::SessionState;
use datafusion::execution::context::QueryPlanner;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, ExtensionPlanner, PhysicalPlanner};
use datafusion_common::{Result, internal_err};
use datafusion_expr::{LogicalPlan, UserDefinedLogicalNode};
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
        DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(SparkExtensionPlanner)])
            .create_physical_plan(logical_plan, session_state)
            .await
    }
}

/// Compose this with other host extension planners when constructing a session.
pub struct SparkExtensionPlanner;

#[async_trait]
impl ExtensionPlanner for SparkExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(partition_id) = node.as_any().downcast_ref::<SparkPartitionIdNode>() else {
            return Ok(None);
        };
        let [input] = physical_inputs else {
            return internal_err!("SparkPartitionIdExec requires exactly one physical input");
        };
        Ok(Some(Arc::new(SparkPartitionIdExec::try_new(
            input.clone(),
            partition_id.column_name(),
            node.schema().inner().clone(),
        )?)))
    }
}
