//! Bind RAND occurrences to native projection and filter execution streams.

use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{
    Distribution, OrderingRequirements, PhysicalExpr, ScalarFunctionExpr,
};
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr, ProjectionExprs};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{Result, Statistics};
use datafusion_expr::ScalarUDF;
use sail_function::scalar::math::random::Random;

fn contains_rand(expression: &Arc<dyn PhysicalExpr>) -> Result<bool> {
    expression
        .exists(|node| Ok(ScalarFunctionExpr::try_downcast_func::<Random>(node.as_ref()).is_some()))
}

fn bind_rand(
    expression: Arc<dyn PhysicalExpr>,
    schema: &Schema,
    partition: usize,
) -> Result<Arc<dyn PhysicalExpr>> {
    expression
        .transform_up(|expression| {
            let Some(random) = ScalarFunctionExpr::try_downcast_func::<Random>(expression.as_ref())
            else {
                return Ok(Transformed::no(expression));
            };
            let bound = ScalarFunctionExpr::new(
                random.name(),
                Arc::new(ScalarUDF::from(Random::for_partition(partition))),
                random.args().to_vec(),
                random.return_field(schema)?,
                Arc::new(random.config_options().clone()),
            );
            Ok(Transformed::yes(Arc::new(bound) as Arc<dyn PhysicalExpr>))
        })
        .map(|expression| expression.data)
}

/// Install after native optimization so binding does not change expression placement.
pub(super) fn bind_seeded_rand_streams(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    plan.transform_up(wrap_rand_operator).map(|plan| plan.data)
}

fn wrap_rand_operator(plan: Arc<dyn ExecutionPlan>) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
    if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
        for expression in projection.expr() {
            if contains_rand(&expression.expr)? {
                return Ok(Transformed::yes(
                    Arc::new(SeededRandExec::Projection(projection.clone()))
                        as Arc<dyn ExecutionPlan>,
                ));
            }
        }
    } else if let Some(filter) = plan.downcast_ref::<FilterExec>()
        && contains_rand(filter.predicate())?
    {
        return Ok(Transformed::yes(
            Arc::new(SeededRandExec::Filter(filter.clone())) as Arc<dyn ExecutionPlan>,
        ));
    }
    Ok(Transformed::no(plan))
}

#[derive(Debug)]
enum SeededRandExec {
    Projection(ProjectionExec),
    Filter(FilterExec),
}

impl SeededRandExec {
    fn native(&self) -> &dyn ExecutionPlan {
        match self {
            Self::Projection(projection) => projection,
            Self::Filter(filter) => filter,
        }
    }
}

impl DisplayAs for SeededRandExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "SeededRandExec: ")?;
        self.native().fmt_as(t, f)
    }
}

impl ExecutionPlan for SeededRandExec {
    fn name(&self) -> &'static str {
        "SeededRandExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.native().properties()
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        self.native().required_input_distribution()
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        self.native().required_input_ordering()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        self.native().maintains_input_order()
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        self.native().benefits_from_input_partitioning()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.native().children()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let rebuilt = match self.as_ref() {
            Self::Projection(projection) => {
                Arc::new(projection.clone()).with_new_children(children)?
            }
            Self::Filter(filter) => Arc::new(filter.clone()).with_new_children(children)?,
        };
        wrap_rand_operator(rebuilt).map(|plan| plan.data)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        match self {
            Self::Projection(projection) => {
                let schema = projection.input().schema();
                let expressions = projection
                    .expr()
                    .iter()
                    .map(|expression| {
                        Ok(ProjectionExpr::new(
                            bind_rand(Arc::clone(&expression.expr), &schema, partition)?,
                            expression.alias.clone(),
                        ))
                    })
                    .collect::<Result<ProjectionExprs>>()?;
                let projector = expressions.make_projector(&schema)?;
                projection.execute_with_projector(&projector, partition, context)
            }
            Self::Filter(filter) => {
                let predicate = bind_rand(
                    Arc::clone(filter.predicate()),
                    &filter.input().schema(),
                    partition,
                )?;
                filter.execute_with_predicate(predicate, partition, context)
            }
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.native().metrics()
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        self.native().partition_statistics(partition)
    }

    fn fetch(&self) -> Option<usize> {
        self.native().fetch()
    }

    fn supports_limit_pushdown(&self) -> bool {
        self.native().supports_limit_pushdown()
    }

    fn with_fetch(&self, fetch: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        let native = self.native().with_fetch(fetch)?;
        let bound = match self {
            Self::Projection(_) => {
                Self::Projection(native.downcast_ref::<ProjectionExec>()?.clone())
            }
            Self::Filter(_) => Self::Filter(native.downcast_ref::<FilterExec>()?.clone()),
        };
        Some(Arc::new(bound))
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        self.native().cardinality_effect()
    }
}
