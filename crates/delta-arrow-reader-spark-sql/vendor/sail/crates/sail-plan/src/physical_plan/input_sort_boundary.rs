//! Keep a retained Spark input sort below its row-error consumer.

use std::sync::Arc;

use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::Distribution;
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_common::{Result, Statistics, internal_err};

/// Own the sort so physical rewrites cannot move its consumer below it.
/// Sorting, spilling and errors remain SortExec's.
#[derive(Debug)]
pub(super) struct InputSortBoundaryExec {
    sort: SortExec,
}

impl InputSortBoundaryExec {
    pub(super) fn new(sort: SortExec) -> Self {
        Self { sort }
    }

    fn from_rebuilt_sort(sort: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        let Some(sort) = sort.downcast_ref::<SortExec>() else {
            return internal_err!("InputSortBoundaryExec expected a rebuilt SortExec");
        };
        Ok(Arc::new(Self::new(sort.clone())))
    }
}

impl DisplayAs for InputSortBoundaryExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "InputSortBoundaryExec: ")?;
        self.sort.fmt_as(t, f)
    }
}

impl ExecutionPlan for InputSortBoundaryExec {
    fn name(&self) -> &'static str {
        "InputSortBoundaryExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.sort.properties()
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        self.sort.required_input_distribution()
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        self.sort.benefits_from_input_partitioning()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![self.sort.input()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return internal_err!("InputSortBoundaryExec requires exactly one child");
        }
        Self::from_rebuilt_sort(Arc::new(self.sort.clone()).with_new_children(children)?)
    }

    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        Self::from_rebuilt_sort(Arc::new(self.sort.clone()).reset_state()?)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // shortcut: retain SortExec's already-ordered bypass; revisit for an independently verified full-input contract.
        self.sort.execute(partition, context)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.sort.metrics()
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        self.sort.partition_statistics(partition)
    }

    fn fetch(&self) -> Option<usize> {
        self.sort.fetch()
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        self.sort.cardinality_effect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::compute::SortOptions;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::config::ConfigOptions;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::{LexOrdering, PhysicalExpr, PhysicalSortExpr};
    use datafusion::physical_optimizer::PhysicalOptimizerRule;
    use datafusion::physical_optimizer::enforce_sorting::EnforceSorting;
    use datafusion::physical_optimizer::projection_pushdown::ProjectionPushdown;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::projection::ProjectionExec;

    #[test]
    fn input_sort_boundary_blocks_the_real_projection_swap_and_survives_rebuilds() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("hidden", DataType::Int32, true),
            Field::new("sort_key", DataType::Int32, false),
            Field::new("array_value", DataType::Int32, true),
            Field::new("other", DataType::Int32, true),
        ]));
        let input: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(schema));
        let Some(ordering) = LexOrdering::new(vec![PhysicalSortExpr {
            expr: Arc::new(Column::new("sort_key", 1)),
            options: SortOptions {
                descending: true,
                nulls_first: false,
            },
        }]) else {
            return internal_err!("test requires an ordering");
        };
        let sort = SortExec::new(ordering, input.clone()).with_preserve_partitioning(true);
        let projection = |input| -> Result<Arc<dyn ExecutionPlan>> {
            Ok(Arc::new(ProjectionExec::try_new(
                vec![
                    (
                        Arc::new(Column::new("array_value", 2)) as Arc<dyn PhysicalExpr>,
                        "renamed_array".to_owned(),
                    ),
                    (
                        Arc::new(Column::new("sort_key", 1)) as Arc<dyn PhysicalExpr>,
                        "renamed_key".to_owned(),
                    ),
                    (
                        Arc::new(Column::new("other", 3)) as Arc<dyn PhysicalExpr>,
                        "renamed_other".to_owned(),
                    ),
                ],
                input,
            )?))
        };
        let options = ConfigOptions::default();
        let ordinary =
            ProjectionPushdown::new().optimize(projection(Arc::new(sort.clone()))?, &options)?;
        assert!(
            ordinary.downcast_ref::<SortExec>().is_some(),
            "ordinary Sort must still swap"
        );
        assert!(
            ordinary.children()[0]
                .downcast_ref::<ProjectionExec>()
                .is_some()
        );

        let boundary: Arc<dyn ExecutionPlan> = Arc::new(InputSortBoundaryExec::new(sort));
        let rebuilt = boundary.clone().with_new_children(vec![input.clone()])?;
        assert_eq!(rebuilt.schema(), boundary.schema());
        assert!(matches!(
            rebuilt.required_input_distribution().as_slice(),
            [Distribution::UnspecifiedDistribution]
        ));
        assert_eq!(
            rebuilt.properties().output_ordering(),
            boundary.properties().output_ordering()
        );
        assert!(boundary.clone().with_new_children(vec![]).is_err());
        let reset = rebuilt.reset_state()?;
        assert_eq!(reset.name(), "InputSortBoundaryExec");
        assert!(reset.children()[0].downcast_ref::<EmptyExec>().is_some());
        let mut protected = projection(reset)?;
        for _ in 0..2 {
            protected = EnforceSorting::new().optimize(protected, &options)?;
            protected = ProjectionPushdown::new().optimize(protected, &options)?;
            assert!(protected.downcast_ref::<ProjectionExec>().is_some());
            assert_eq!(protected.children()[0].name(), "InputSortBoundaryExec");
            assert!(
                protected.children()[0].children()[0]
                    .downcast_ref::<EmptyExec>()
                    .is_some()
            );
        }
        Ok(())
    }
}
